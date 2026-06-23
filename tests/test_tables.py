"""Table-function tests via the in-process buffering harness.

Drive ``match_resolve`` through the real bind -> process(sink) -> combine ->
finalize lifecycle (no subprocess), checking the emitted Arrow result: the
dynamic passthrough+append output schema, and that planted duplicates cluster.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from vgi_match.tables import MatchResolve

from .harness import run_buffering
from .synthetic import EXPECTED_GROUPS, make_customers


def _arrow(df) -> pa.Table:
    return pa.Table.from_pandas(df, preserve_index=False)


def test_output_schema_is_passthrough_plus_two() -> None:
    df = make_customers()
    out = run_buffering(MatchResolve, _arrow(df), named={"columns": "first_name,last_name,email"})
    # Every input column is passed through, then cluster_id + match_probability.
    assert out.schema.names == [*df.columns, "cluster_id", "match_probability"]
    assert pa.types.is_string(out.schema.field("cluster_id").type)
    assert pa.types.is_floating(out.schema.field("match_probability").type)
    assert out.num_rows == len(df)


def test_duplicates_cluster_together() -> None:
    df = make_customers()
    out = run_buffering(MatchResolve, _arrow(df), named={"columns": "first_name,last_name,email"})
    d = out.to_pydict()
    by_row = dict(zip(d["row_id"], d["cluster_id"], strict=True))

    for group in EXPECTED_GROUPS:
        ids = {by_row[r] for r in group}
        assert len(ids) == 1, f"group {group} split across clusters {ids}"

    group_cluster = [by_row[g[0]] for g in EXPECTED_GROUPS]
    assert len(set(group_cluster)) == len(EXPECTED_GROUPS)


def test_missing_column_raises() -> None:
    df = make_customers()
    with pytest.raises(Exception, match="not found"):
        run_buffering(MatchResolve, _arrow(df), named={"columns": "first_name,nope"})
