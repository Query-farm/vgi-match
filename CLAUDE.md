# CLAUDE.md — vgi-match

Contributor/agent notes. User-facing docs live in `README.md`; this is the
"how it's built and where the sharp edges are" companion. Sibling style/tooling
to `vgi-causal` (structure) and `vgi-embed` (the model-warm-up startup pattern);
the whole-relation buffering data-flow mirrors `vgi-causal`.

## What this is

A [VGI](https://query.farm) worker exposing **probabilistic entity resolution /
record linkage / dedup** to DuckDB/SQL, backed by
[Splink](https://github.com/moj-analytical-services/splink) (MIT). One table
function — `match_resolve` — buffers a whole relation, runs Splink's
Fellegi-Sunter model over it, and returns the input rows unchanged plus an
appended `cluster_id` (same entity = same id) and `match_probability`.
`match_worker.py` assembles it into one `match` catalog (single `main` schema)
over stdio.

## Layout

```
match_worker.py        repo-root stdio entry shim; PEP 723 inline deps; main()
vgi_match/
  linkage.py           pure Splink linkage logic over pandas frames; no Arrow/VGI; unit-testable
  buffering.py         SinkBuffer (single-bucket sink/combine) + Arrow<->pandas plumbing
  tables.py            the MatchResolve TableBufferingFunction + DYNAMIC output schema + args
  schema_utils.py      pa.Field comment / column-doc helper
  worker.py            assembles the catalog; main() / main_http(); Splink warm-up
tests/
  synthetic.py         deterministic customer fixture with KNOWN duplicate groups (the validation backbone)
  test_linkage.py      pure-logic: planted groups recovered, validation errors
  test_tables.py       in-process buffering harness (dynamic schema + clustering)
  test_client.py       real Client RPC subprocess (how DuckDB drives it)
test/sql/match.test    haybarn-unittest sqllogictest — authoritative E2E
Makefile               test / test-unit / test-sql / lint
```

To add a function: implement the logic in `linkage.py` (pure, takes a pandas
frame + config, returns a `dict[str, list]`, raises `MatchError` on bad input),
add an `@dataclass` args class + a `SinkBuffer` subclass in `tables.py`, append
it to `TABLE_FUNCTIONS`.

## THE core convention (read first): one relation in, rows + cluster_id out

`match_resolve` is a **table function**, not a scalar. It takes the whole input
relation as a single `(SELECT ...)` subquery — `Arg(0)`, typed `TableInput` —
and the linkage config as NAMED args (`columns := 'first_name,last_name,email'`,
`threshold := 0.9`, `train := false`). **Every column not in `columns` is passed
straight through** to the output; the comparison columns are passed through too
and additionally drive matching.

Entity resolution is an all-pairs whole-relation problem, so `match_resolve` is
a `TableBufferingFunction` (Sink+Source):

- `process(batch)` — sink each input batch to execution-scoped `BoundStorage`.
- `combine(state_ids)` — collapse to a single finalize key (one bucket).
- `finalize(...)` — reassemble the full table (`buffered_frame()` → pandas), run
  Splink once, emit one result batch, then `out.finish()`.

`SinkBuffer` in `buffering.py` implements `process`/`combine`/`buffered_frame`;
`MatchResolve` only writes `on_bind` (the dynamic output schema) + `finalize`. A
`DrainState(done: bool)` cursor makes finalize emit exactly once.

## The dynamic output schema (the one non-obvious VGI bit)

Unlike `vgi-causal`'s fixed output schemas, `match_resolve`'s output schema is
**input-schema-dependent**: it's the `(SELECT ...)` relation's own schema
(passthrough) with `cluster_id` (VARCHAR) + `match_probability` (DOUBLE)
appended. We build it in `on_bind` from `params.bind_call.input_schema` (the
documented pass-through-plus-extra pattern; see `_output_schema` in `tables.py`).
The two appended names are reserved — an input column already named `cluster_id`
/ `match_probability` is dropped in favor of the produced one.

## The linkage (the Splink bit) — and the training decision

All in `linkage.py`, pure functions over a pandas frame:

- **Private unique id.** Splink requires a unique id; we never trust the caller's
  columns, so we synthesize `__vgi_match_uid` (0..n-1), use it through Splink,
  then drop it and restore **input row order** from it before emitting.
- **Default comparisons.** Name-like columns (`first_name`, `last_name`,
  `surname`, …) → fuzzy `NameComparison` (Jaro-Winkler); everything else →
  `LevenshteinAtThresholds([1,2])`. These library comparisons carry
  well-calibrated **default per-level m/u** probabilities.
- **Blocking** on each comparison column in turn (a pair is a candidate if it
  agrees on *any one* column) — keeps the candidate set sub-quadratic.
- **Predict → cluster.** `predict(threshold)` then
  `cluster_pairwise_predictions_at_threshold(threshold)`; connected components
  become `cluster_id`. We re-map Splink's arbitrary internal cluster ids to
  stable string ids (`_stable_cluster_strings`).
- **`match_probability`** per row = the strongest pairwise probability touching
  it (`_row_match_probability`); singletons get `1.0`.

### Why the default model is NOT EM-trained (learned the hard way)

The honest training/serving split: training/tuning is **Splink's** job; the
worker consumes settings + data. Three model paths:

1. **Default (no training)** — built from `columns` with the library comparisons'
   default m/u + a `prior` (`probability_two_random_records_match=0.1`). This is
   **deterministic and robust on a relation of any size**, including the 7-row
   E2E fixture.
2. **`train := true`** — additionally runs Splink **unsupervised EM**. EM is
   genuinely **unreliable on small inputs**: on the 7-row fixture it collapses
   every record to a singleton (match weights → ~1e-300). So EM is **opt-in and
   best-effort** — `_train` tries one EM pass per column, **skips** passes that
   raise (a column whose true-match pairs all *disagree* yields an empty blocked
   comparison set and Splink raises), and if none succeed we **keep the
   default** model rather than error.
3. **`settings_json`** (Python-API only for now) — a pre-trained Splink settings
   dict/JSON used **as-is**, no training. The serving/reuse path.

> If you "improve" the default to always EM-train, the E2E fixture will regress
> to all-singletons. Don't. Keep default `train=False`.

## Validation: a planted-duplicate fixture (read `tests/synthetic.py`)

`make_customers` returns 7 customer records with a known ground-truth partition
(`EXPECTED_GROUPS`): John/Jon/Johnny Smith are one entity, Jane/Janet Doe
another, Robert Jones and Alice Jones singletons (Alice shares a *surname* with
Robert but is a different person — the over-merge trap). Tests assert the planted
groups land in one cluster each AND distinct entities stay apart. The `.test`
file plants the same story deterministically as a `VALUES` relation.

## Sharp edges

1. **`haybarn-unittest` silently SKIPS `require vgi`.** Use an explicit
   `statement ok` / `LOAD vgi;` (the `.test` here does). `# group: [vgi_match]`
   and `require-env VGI_MATCH_WORKER` gate the file; ATTACH via
   `'${VGI_MATCH_WORKER}'`. Run with the glob `test/sql/*`.
2. **Splink logs would corrupt the stdio protocol.** Splink/DuckDB are *chatty*
   (EM iterations, untrained-level warnings). `linkage.resolve` and the worker's
   warm-up both set `logging.getLogger("splink").setLevel(ERROR)` so nothing
   leaks onto the stdio VGI stream. Keep it that way.
3. **Warm-up.** `MatchWorker.run` imports Splink at process spawn (`_warm_up`),
   best-effort, so the first `match_resolve` doesn't pay the multi-hundred-ms
   import inline (which under the E2E suite risks a spurious teardown failure).
4. **Buffering needs the input schema at bind.** The relation's schema arrives
   via `bind_call.input_schema`; `on_bind` builds the output schema from it and
   `buffered_frame()` uses it to reassemble even when zero batches were sunk.
5. **Determinism.** `estimate_u_using_random_sampling(seed=1)` and a fixed prior
   keep the (opt-in) trained path reproducible. The default path has no RNG.

## Memory characteristic (state it honestly)

**Splink runs its OWN DuckDB inside this worker.** Rows arrive over Arrow from
the caller's DuckDB, are buffered into one pandas frame, and are handed to
Splink's DuckDB. So this is **buffer-all-then-compute**: peak worker memory holds
the whole input relation plus Splink's intermediate blocked-pairs tables. Right
shape for the small/medium relations entity resolution runs on; **not** a
streaming operator — block tightly and partition for very large inputs.

## Gaps / deferred (be honest)

- **`match_score` serving function is NOT implemented as a SQL function.** The
  design supports it — `resolve(..., settings_json=...)` already loads & reuses a
  pre-trained settings object — but scoring a *single new record* against a
  stored model without re-clustering the whole relation is a planned extension,
  not yet wired to a `match.match_score(...)` table function.
- **`settings_json` is Python-API only.** Not yet exposed as a SQL named arg
  (would need a VARCHAR JSON / secret-backed model reference).
- **`link_type` is `dedupe_only`.** Two-source linking (`link_only`) is not
  exposed.

## Licensing

`vgi-match` is MIT. Splink is MIT; its DuckDB backend is MIT. **`python-igraph`
(a Splink runtime dep for connected-components clustering) is GPL-2.0** — used at
runtime, not linked into our code, but redistributors bundling the full closure
should note it. See `README.md` for the full table.
