"""End-to-end tests driving match_worker.py as a real subprocess.

These spawn the worker via ``vgi.client.Client`` and invoke ``match_resolve``
through the real ``table_buffering_function`` RPC path -- exactly how DuckDB
drives a buffering function after ``ATTACH`` -- exercising bind, the sink
process RPC per batch, combine, and the finalize source stream over the wire.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from vgi import Arguments
from vgi.client import Client, ClientError

from .synthetic import EXPECTED_GROUPS, make_customers

_WORKER = str(Path(__file__).resolve().parent.parent / "match_worker.py")


@pytest.fixture(scope="module")
def client() -> Iterator[Client]:
    with Client(f"{sys.executable} {_WORKER}", worker_limit=1) as c:
        yield c


def _run(client: Client, table: pa.Table, **named: object) -> pa.Table:
    batches = list(
        client.table_buffering_function(
            function_name="match_resolve",
            input=iter(table.to_batches()),
            arguments=Arguments(named={k: pa.scalar(v) for k, v in named.items()}),
        )
    )
    return pa.Table.from_batches(batches)


def test_match_resolve_e2e(client: Client) -> None:
    df = make_customers()
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    out = _run(client, tbl, columns="first_name,last_name,email")
    d = out.to_pydict()
    assert out.schema.names == [*df.columns, "cluster_id", "match_probability"]

    by_row = dict(zip(d["row_id"], d["cluster_id"], strict=True))
    for group in EXPECTED_GROUPS:
        assert len({by_row[r] for r in group}) == 1
    assert len({by_row[g[0]] for g in EXPECTED_GROUPS}) == len(EXPECTED_GROUPS)


def test_missing_column_errors_e2e(client: Client) -> None:
    df = make_customers()
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    with pytest.raises(ClientError):
        _run(client, tbl, columns="first_name,nope")
