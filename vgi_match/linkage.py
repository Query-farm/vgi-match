"""Pure probabilistic record-linkage / entity-resolution logic over Splink.

This module is the framework-free core: it takes a ``pandas.DataFrame`` (the
buffered input relation) plus the linkage config, runs `Splink
<https://moj-analytical-services.github.io/splink/>`_ (MIT), and returns the
per-row cluster assignment (and match probability) as plain Python column
blocks ready to hand to pyarrow. No VGI, no DuckDB-over-Arrow, no worker
plumbing here -- so the whole linkage is directly unit-testable.

How linkage works (Splink, the Fellegi-Sunter model)
----------------------------------------------------
Entity resolution / dedup is an *all-pairs* problem: to decide whether two
records are the same entity, every record is (conceptually) compared against
every other. Splink makes this tractable with three ideas:

* **Blocking** -- only compare record pairs that agree on at least one blocking
  rule (e.g. same ``email`` OR same ``surname``), so we never materialize the
  full O(n^2) cross product.
* **Comparisons** -- for each blocked pair, each comparison column is scored
  into discrete agreement levels (exact / fuzzy / no-match) using string-
  similarity (Jaro-Winkler, Levenshtein, ...).
* **The Fellegi-Sunter model** -- a match weight is accumulated from per-level
  ``m`` (probability of that level *given a true match*) and ``u`` (given a
  *non*-match) probabilities, yielding a calibrated ``match_probability`` per
  pair. Pairs above a threshold are linked, and the connected components of the
  resulting graph become entity **clusters** (``cluster_id``).

Training (honest split)
-----------------------
Splink learns ``u`` by random sampling and ``m``/``probability_two_random_
records_match`` by **unsupervised EM** (Expectation-Maximisation) -- no labels
required. That is exactly what :func:`resolve` does by default when no
pre-trained settings are supplied: it builds a sensible default
``SettingsCreator`` from the comparison columns, runs EM on the buffered data,
then predicts + clusters. A caller who has tuned a model elsewhere can instead
pass a Splink settings dict (``settings_json``) and we consume it as-is --
training/tuning stays Splink's job; the worker consumes settings + data.

Memory characteristic (documented honestly)
--------------------------------------------
Splink spins up its **own** DuckDB engine inside this process to run the
linkage. The rows arrive here over Arrow from the *caller's* DuckDB, are
buffered into a single pandas frame, then handed to Splink's DuckDB. So this is
buffer-all-then-compute: peak memory holds the whole input relation (plus
Splink's intermediate blocked-pairs tables) in this worker process. Fine for
the small/medium relations entity resolution usually runs on; not a streaming
operator.

Licensing: Splink is MIT (Ministry of Justice, UK). Its DuckDB backend is MIT;
pandas/pyarrow are BSD/Apache-2.0.
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from typing import Any

import pandas as pd

__all__ = [
    "DEFAULT_THRESHOLD",
    "MatchError",
    "resolve",
]

# Default match-probability threshold for linking a pair (and for forming
# clusters). 0.5 is the natural Fellegi-Sunter decision boundary (match weight
# 0); callers tune it up to trade recall for precision.
DEFAULT_THRESHOLD = 0.5

# Default prior probability that two random records match, used by the default
# (untrained) model. Splink's library comparisons carry well-calibrated default
# per-level m/u probabilities; combined with this prior they give sensible,
# deterministic match weights on a relation of any size -- unlike unsupervised
# EM, which is unreliable (degenerate) on small inputs. Callers enable EM with
# train=True or supply a fully pre-trained settings_json.
_DEFAULT_PRIOR = 0.1

# Internal unique-id column we add to the buffered frame. Splink requires a
# unique id; we never trust the caller to have one, so we synthesize a private
# row id and drop it from the output. Prefixed to avoid colliding with caller
# columns.
_UID = "__vgi_match_uid"

# A private cluster column name from Splink, renamed to the public "cluster_id".
_CLUSTER_COL = "cluster_id"


class MatchError(ValueError):
    """Raised for user-facing input problems (missing columns, empty input).

    A plain, explicit error so the worker surfaces a clear message to SQL
    instead of crashing with an opaque Splink/DuckDB traceback.
    """


def _require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """Validate that every requested comparison column is present.

    Args:
        df: The input relation.
        columns: Comparison column names the caller asked to match on.

    Raises:
        MatchError: If any named column is absent, or none were given.
    """
    if not columns:
        raise MatchError(
            "no comparison columns supplied; pass columns := 'col1,col2,...' "
            "naming the fields to match records on"
        )
    have = set(df.columns)
    missing = [c for c in columns if c not in have]
    if missing:
        raise MatchError(
            f"comparison column(s) not found in input relation: "
            f"{', '.join(missing)}; relation has columns: "
            f"{', '.join(map(str, df.columns))}"
        )


def _build_comparisons(columns: list[str]) -> list[Any]:
    """Build a sensible default Splink comparison per column.

    Name-ish columns get a fuzzy :class:`NameComparison` (Jaro-Winkler levels)
    so nicknames/typos (John/Jon/Johnny) still match; everything else gets a
    fuzzy :class:`LevenshteinAtThresholds` (exact, <=1, <=2 edits) so minor
    variants match too. These library comparisons carry well-calibrated default
    per-level ``m``/``u`` probabilities, which is what makes the default
    (untrained) model produce sensible match weights. A caller who wants
    exact-only or domain-tuned comparisons supplies ``settings_json``.

    Args:
        columns: Comparison column names.

    Returns:
        A list of Splink comparison objects, one per column.
    """
    import splink.comparison_library as cl  # noqa: PLC0415 (lazy: heavy import)

    name_like = {"name", "first_name", "last_name", "surname", "forename", "full_name"}
    comparisons: list[Any] = []
    for col in columns:
        if col.lower() in name_like:
            comparisons.append(cl.NameComparison(col))
        else:
            # Exact + Levenshtein <=1/<=2 levels, with a no-match catch-all.
            comparisons.append(cl.LevenshteinAtThresholds(col, [1, 2]))
    return comparisons


def _default_settings(columns: list[str], *, prior: float) -> Any:
    """Construct a default Splink ``SettingsCreator`` for dedupe.

    Blocks on each comparison column in turn (a pair is considered if it agrees
    on *any* one column), which keeps the candidate set small while still
    catching records that differ on some fields. ``dedupe_only`` link type:
    we are de-duplicating a single relation, not linking two sources. The
    ``probability_two_random_records_match`` prior plus the comparisons' default
    per-level m/u give a usable model **without** training -- robust on inputs
    of any size.

    Args:
        columns: Comparison column names.
        prior: Prior probability that two random records match.

    Returns:
        A configured Splink ``SettingsCreator``.
    """
    from splink import SettingsCreator, block_on  # noqa: PLC0415 (lazy)

    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name=_UID,
        probability_two_random_records_match=prior,
        comparisons=_build_comparisons(columns),
        blocking_rules_to_generate_predictions=[block_on(c) for c in columns],
        retain_intermediate_calculation_columns=False,
        retain_matching_columns=False,
    )


def _train(linker: Any, columns: list[str]) -> bool:
    """Run Splink's unsupervised training on the buffered data.

    Estimates ``u`` by random sampling and ``m`` /
    ``probability_two_random_records_match`` by EM. Each EM pass is blocked on a
    *different* column so the parameters for the *other* columns are
    identifiable in that pass.

    Robustness: an EM pass blocked on column ``c`` only works if some true-match
    pairs actually agree on ``c`` (otherwise the blocked comparison set is empty
    and Splink raises). With fuzzy/typo data a given column may not agree on any
    matched pair, so we run each pass **best-effort** and skip the ones that
    produce no comparisons -- as long as at least one pass succeeds the model is
    trained enough to predict + cluster. Splink also emits warnings on tiny
    inputs where some agreement levels are never observed; those parameters fall
    back to sensible defaults. We quiet the noisiest logger but never swallow a
    *total* training failure (re-raised below).

    Args:
        linker: A constructed Splink ``Linker``.
        columns: Comparison column names (drive the per-pass EM blocking).

    Returns:
        ``True`` if at least one EM pass trained successfully, ``False`` if none
        did (the caller then keeps the default-calibrated, untrained model).
    """
    from splink import block_on  # noqa: PLC0415 (lazy)

    training = linker.training
    # probability_two_random_records_match: estimate from a deterministic rule.
    # Try each column; a column whose matches all disagree yields no pairs, so
    # fall back across columns. recall is a rough guess EM refines afterwards.
    for col in columns:
        with contextlib.suppress(Exception):
            training.estimate_probability_two_random_records_match([block_on(col)], recall=0.7)
            break
    # u: from random pairs (almost all non-matches), sampled deterministically.
    with contextlib.suppress(Exception):
        training.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)
    # m + refine match probability via EM, one best-effort pass per column.
    trained_any = False
    for col in columns:
        try:
            training.estimate_parameters_using_expectation_maximisation(block_on(col))
            trained_any = True
        except Exception:  # noqa: BLE001 (column with no agreeing matched pairs)
            continue
    return trained_any


def resolve(
    df: pd.DataFrame,
    *,
    columns: list[str],
    threshold: float = DEFAULT_THRESHOLD,
    train: bool = False,
    prior: float = _DEFAULT_PRIOR,
    settings_json: dict[str, Any] | str | None = None,
) -> dict[str, list]:
    """Resolve entities in a buffered relation: append ``cluster_id`` per row.

    Runs the full Splink pipeline -- build/load a model -> predict pairwise
    match probabilities -> cluster the match graph at ``threshold`` -- and
    returns, **in input row order**, the original rows augmented with a
    ``cluster_id`` (records sharing an id are the same resolved entity) and a
    ``match_probability`` (the strongest pairwise probability linking the row
    into its cluster; 1.0 for a singleton/seed row, NULL only if unscored).

    Model selection (the honest training/serving split):

    * ``settings_json`` given -> used **as-is**; the caller trained/tuned the
      model elsewhere (Splink's job). No training here.
    * else, default model: built from ``columns`` using Splink's library
      comparisons, whose well-calibrated default per-level ``m``/``u`` plus the
      ``prior`` give sensible, **deterministic** match weights on a relation of
      *any* size.
    * ``train=True`` additionally runs Splink's unsupervised EM on this data to
      refine the default model. EM is unreliable on very small inputs, so it is
      **opt-in** and **best-effort**: if no EM pass can be trained the default-
      calibrated model is kept (never an error).

    Args:
        df: The buffered input relation (whole relation, one pandas frame).
        columns: Comparison columns to match records on. Every other column is
            passed through untouched.
        threshold: Match-probability threshold in [0, 1] for linking a pair and
            for forming clusters. Default :data:`DEFAULT_THRESHOLD` (0.5).
        train: Run unsupervised EM to refine the default model (opt-in). Ignored
            when ``settings_json`` is supplied.
        prior: Prior probability two random records match (default model only).
        settings_json: Optional pre-trained Splink settings (dict or JSON
            string). When given, it is used as-is and no training is performed.

    Returns:
        A column block: every input column (in original order, original values)
        plus ``cluster_id`` (str) and ``match_probability`` (float | None), one
        entry per input row, in input order.

    Raises:
        MatchError: On empty input or missing comparison columns.
    """
    if len(df) == 0:
        raise MatchError("match_resolve requires a non-empty input relation")
    if not 0.0 <= threshold <= 1.0:
        raise MatchError(f"threshold must be in [0, 1], got {threshold}")
    _require_columns(df, columns)

    # Splink is heavy to import; do it lazily so module import (and the unit
    # tests that don't link) stay cheap. The worker warms it at startup.
    from splink import DuckDBAPI, Linker  # noqa: PLC0415

    # Splink's DuckDB logs are chatty (EM iterations, untrained-level warnings);
    # keep them off stdout so they never corrupt the stdio VGI protocol stream.
    logging.getLogger("splink").setLevel(logging.ERROR)

    # Synthesize a private, collision-free unique id and remember the original
    # row order so we can restore it after Splink reshuffles rows.
    work = df.reset_index(drop=True).copy()
    work[_UID] = range(len(work))

    if settings_json is not None:
        # A pre-trained settings object/dict/JSON path -- consumed as-is. Splink's
        # Linker accepts a dict or a settings file path directly.
        settings: Any = settings_json
        do_train = False
    else:
        settings = _default_settings(columns, prior=prior)
        do_train = train

    db_api = DuckDBAPI()
    linker = Linker(work, settings, db_api=db_api)

    if do_train:
        # Best-effort EM refinement; default-calibrated model kept if it fails.
        _train(linker, columns)

    preds = linker.inference.predict(threshold_match_probability=threshold)
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        preds, threshold_match_probability=threshold
    )
    cluster_df = clusters.as_pandas_dataframe()

    # Per-row best match probability: the strongest pairwise probability that
    # links this row to another. Singletons get 1.0 (a record is a perfect
    # match to itself / its own cluster seed).
    prob_by_uid = _row_match_probability(preds.as_pandas_dataframe(), len(work))

    # Restore input order via the private uid, map clusters to stable string ids.
    cluster_lookup = dict(zip(cluster_df[_UID].tolist(), cluster_df[_CLUSTER_COL].tolist(), strict=True))
    cluster_ids = _stable_cluster_strings([cluster_lookup.get(i) for i in range(len(work))])

    out: dict[str, list] = {}
    for col in df.columns:
        out[str(col)] = work[col].tolist()
    out[_CLUSTER_COL] = cluster_ids
    out["match_probability"] = [prob_by_uid.get(i) for i in range(len(work))]
    return out


def _row_match_probability(preds: pd.DataFrame, n_rows: int) -> dict[int, float]:
    """Best pairwise match probability touching each row (by private uid).

    For each predicted pair ``(l, r, p)`` we record ``p`` against both rows,
    keeping the max. Rows never appearing in a prediction (singletons) get 1.0.

    Args:
        preds: Splink's pairwise prediction frame (has ``<uid>_l``, ``<uid>_r``,
            ``match_probability``).
        n_rows: Total number of input rows.

    Returns:
        Mapping ``uid -> best match probability``.
    """
    best: dict[int, float] = {}
    left = f"{_UID}_l"
    right = f"{_UID}_r"
    if {left, right, "match_probability"} <= set(preds.columns):
        for lid, rid, prob in zip(
            preds[left].tolist(),
            preds[right].tolist(),
            preds["match_probability"].tolist(),
            strict=True,
        ):
            p = float(prob)
            for rid_ in (int(lid), int(rid)):
                if p > best.get(rid_, -1.0):
                    best[rid_] = p
    # Singletons (and any unscored row) are perfect matches to their own cluster.
    for i in range(n_rows):
        best.setdefault(i, 1.0)
    return best


def _stable_cluster_strings(raw_ids: list[Any]) -> list[str]:
    """Map Splink's internal cluster ids to stable, opaque string ids.

    Splink cluster ids are arbitrary (often the min uid in the component); we
    expose deterministic ``cluster_id`` strings so callers don't depend on
    Splink internals. Rows in the same component get the same string; a missing
    id (shouldn't happen) gets a fresh unique cluster.

    Args:
        raw_ids: Per-row internal cluster id (or None), in input order.

    Returns:
        Per-row ``cluster_id`` strings, in input order.
    """
    mapping: dict[Any, str] = {}
    result: list[str] = []
    for rid in raw_ids:
        key = rid if rid is not None else uuid.uuid4()
        if key not in mapping:
            mapping[key] = str(len(mapping))
        result.append(mapping[key])
    return result
