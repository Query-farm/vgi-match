"""Entity-resolution table function(s) for DuckDB via VGI.

``match_resolve`` consumes a *whole* input relation -- passed as a
``(SELECT ...)`` subquery (positional ``Arg(0)``) -- plus the linkage config as
NAMED args (``columns := 'first_name,last_name,email'``, ``threshold := 0.9``).
It buffers every input batch, runs `Splink <https://github.com/moj-analytical-
services/splink>`_ once over the full relation, and emits the **input rows
unchanged** with two appended columns: ``cluster_id`` (records sharing it are
the same resolved entity) and ``match_probability``.

    SELECT * FROM match.match_resolve(
        (SELECT * FROM customers),
        columns := 'first_name,last_name,email'
    ) ORDER BY cluster_id;

Because resolving entities is an all-pairs problem, this is a buffering
(Sink+Source) function: it sinks all input batches, then runs Splink once in
finalize. See ``vgi_match.linkage`` for the linkage logic, assumptions, and the
buffer-all memory characteristic.

Dynamic output schema
---------------------
The output schema is **not** fixed: it is the *input* relation's schema
(passthrough) plus ``cluster_id`` + ``match_probability``. We build it in
``on_bind`` from ``params.bind_call.input_schema`` -- the documented
pass-through-plus-extra pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, TableInput
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_buffering_function import TableBufferingParams
from vgi.table_function import BindParams
from vgi_rpc import OutputCollector

from . import linkage
from .buffering import DrainState, SinkBuffer
from .schema_utils import field as mfield

# The two columns match_resolve appends to the passthrough input columns.
_CLUSTER_FIELD = mfield(
    "cluster_id",
    pa.string(),
    "Resolved-entity id: rows sharing a cluster_id are the same entity.",
    nullable=False,
)
_PROB_FIELD = mfield(
    "match_probability",
    pa.float64(),
    "Strongest pairwise match probability linking the row into its cluster (1.0 for a singleton).",
)


def _output_schema(input_schema: pa.Schema) -> pa.Schema:
    """Passthrough every input field, then append cluster_id + match_probability.

    Guards against the (degenerate) case where the input already has a column
    named ``cluster_id`` / ``match_probability`` by not duplicating it -- the
    appended field wins, since that is what the function produces.

    Args:
        input_schema: The ``(SELECT ...)`` relation's Arrow schema.

    Returns:
        The output Arrow schema (input columns + the two appended columns).
    """
    reserved = {"cluster_id", "match_probability"}
    fields = [f for f in input_schema if f.name not in reserved]
    return pa.schema([*fields, _CLUSTER_FIELD, _PROB_FIELD])


def _parse_columns(spec: str) -> list[str]:
    """Parse the comma-separated ``columns`` arg into a clean column list."""
    return [c.strip() for c in spec.split(",") if c.strip()]


@dataclass(slots=True, frozen=True)
class MatchResolveArgs:
    """Bound arguments for ``match_resolve``.

    Attributes:
        data: The relation of records to resolve/dedup (positional ``Arg(0)``).
        columns: Comma-separated comparison columns to match on.
        threshold: Match-probability threshold in ``[0, 1]`` for linking pairs.
        train: Whether to run Splink's opt-in unsupervised EM refinement.
    """

    data: Annotated[
        TableInput,
        Arg(0, doc="Relation of records to resolve/dedup (the rows to cluster)."),
    ]
    columns: Annotated[
        str,
        Arg(
            "columns",
            default="",
            doc="Comma-separated comparison columns to match on, e.g. 'first_name,last_name,email'.",
        ),
    ]
    threshold: Annotated[
        float,
        Arg(
            "threshold",
            default=linkage.DEFAULT_THRESHOLD,
            doc="Match-probability threshold in [0,1] for linking pairs / forming clusters (default 0.5).",
        ),
    ]
    train: Annotated[
        bool,
        Arg(
            "train",
            default=False,
            doc="Run Splink's unsupervised EM to refine the default model (opt-in; weak on tiny inputs).",
        ),
    ]


class MatchResolve(SinkBuffer[MatchResolveArgs, DrainState]):
    """Probabilistic entity resolution / dedup over a buffered relation (Splink)."""

    FunctionArguments: ClassVar[type] = MatchResolveArgs

    class Meta:
        """VGI function metadata (name, description, categories, examples, tags)."""

        name = "match_resolve"
        description = (
            "Probabilistic entity resolution / record linkage / dedup (Splink, MIT). "
            "Buffers the whole input relation, runs Splink's Fellegi-Sunter model "
            "over the comparison columns, and returns the input rows unchanged plus "
            "an appended cluster_id (same entity = same id) and match_probability. "
            "columns := 'col1,col2,...' names the fields to match on; threshold := 0.5 "
            "sets the linking cutoff; train := true opts into unsupervised EM refinement."
        )
        categories = ["entity-resolution", "record-linkage", "dedup"]
        examples = [
            FunctionExample(
                sql=(
                    "SELECT * FROM match.match_resolve((SELECT * FROM customers), "
                    "columns := 'first_name,last_name,email') ORDER BY cluster_id"
                ),
                description="Dedup customers on name + email; rows sharing cluster_id are the same entity",
            ),
            FunctionExample(
                sql=(
                    "SELECT cluster_id, count(*) AS rows_in_entity "
                    "FROM match.match_resolve((SELECT * FROM contacts), "
                    "columns := 'full_name,phone') "
                    "GROUP BY cluster_id HAVING count(*) > 1 ORDER BY rows_in_entity DESC"
                ),
                description="Find duplicate contact groups (clusters with more than one row) and their sizes",
            ),
            FunctionExample(
                sql=(
                    "SELECT * FROM match.match_resolve((SELECT * FROM customers), "
                    "columns := 'first_name,last_name,email', threshold := 0.9, train := true) "
                    "WHERE match_probability >= 0.95"
                ),
                description="Stricter resolution: raise the link threshold, opt into EM training, "
                "keep only high-confidence matches",
            ),
        ]
        tags = {
            "vgi.columns_md": (
                "Returns **every input column unchanged** (passthrough), then appends "
                "these two columns:\n\n"
                "| column | type | description |\n"
                "|---|---|---|\n"
                "| `cluster_id` | VARCHAR | Resolved-entity id; rows sharing a `cluster_id` "
                "are the same entity (a singleton entity gets its own id). |\n"
                "| `match_probability` | DOUBLE | Strongest pairwise match probability "
                "linking the row into its cluster, in `[0, 1]` (`1.0` for a singleton). |\n\n"
                "The passthrough columns vary by input: they are exactly the schema of the "
                "`(SELECT ...)` relation passed as the first argument. If the input already "
                "has a `cluster_id` or `match_probability` column it is replaced by the "
                "produced one."
            ),
        }

    @classmethod
    def on_bind(cls, params: BindParams[MatchResolveArgs]) -> BindResponse:
        """Build the dynamic output schema from the bound input relation's schema.

        Args:
            params: The bind-call context carrying the input relation schema.

        Returns:
            A ``BindResponse`` whose output schema is the passthrough input
            columns plus ``cluster_id`` and ``match_probability``.
        """
        input_schema = params.bind_call.input_schema
        assert input_schema is not None
        return BindResponse(output_schema=_output_schema(input_schema))

    @classmethod
    def initial_finalize_state(
        cls, finalize_state_id: bytes, params: TableBufferingParams[MatchResolveArgs]
    ) -> DrainState:
        """Seed the per-finalize-stream cursor (emit once, then finish).

        Args:
            finalize_state_id: The finalize bucket id.
            params: The buffering-function call context.

        Returns:
            A fresh ``DrainState`` with ``done=False``.
        """
        return DrainState()

    @classmethod
    def finalize(
        cls,
        params: TableBufferingParams[MatchResolveArgs],
        finalize_state_id: bytes,
        state: DrainState,
        out: OutputCollector,
    ) -> None:
        """Reassemble the buffered relation, run Splink once, and emit one batch.

        Args:
            params: The buffering-function call context (args + output schema).
            finalize_state_id: The finalize bucket id.
            state: The drain cursor; ensures the result batch is emitted once.
            out: The output collector to emit the result batch and finish on.

        Raises:
            MatchError: If the buffered input relation has no rows.
        """
        if state.done:
            out.finish()
            return
        state.done = True
        a = params.args
        df = cls.buffered_frame(params)
        if len(df) == 0:
            raise linkage.MatchError("match_resolve requires a non-empty input relation")
        result = linkage.resolve(
            df,
            columns=_parse_columns(a.columns),
            threshold=float(a.threshold),
            train=bool(a.train),
        )
        out.emit(pa.RecordBatch.from_pydict(result, schema=params.output_schema))


TABLE_FUNCTIONS: list[type] = [MatchResolve]
