"""Shared plumbing for the table-buffering entity-resolution functions.

Probabilistic record linkage is an *all-pairs whole-relation* problem: Splink
must see every record before it can decide which records resolve to the same
entity. ``match_resolve`` is therefore a ``TableBufferingFunction``
(Sink+Source) function. The sink phase serializes each input batch to
execution-scoped storage; finalize reassembles the full table and runs Splink
once.

This module holds the single-bucket sink/combine implementation (``SinkBuffer``)
plus the Arrow (de)serialization and a ``pandas`` assembly helper, so each
function only writes its ``finalize`` logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pandas as pd
import pyarrow as pa
from vgi.table_buffering_function import TableBufferingFunction, TableBufferingParams
from vgi_rpc import ArrowSerializableDataclass

_DATA_KEY = b"input_batches"


@dataclass(kw_only=True)
class DrainState(ArrowSerializableDataclass):
    """Per-finalize-stream cursor: emit the single result batch once, then finish."""

    done: bool = False


def serialize_batch(batch: pa.RecordBatch) -> bytes:
    """Serialize one RecordBatch to a self-describing Arrow IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return cast(bytes, sink.getvalue().to_pybytes())


def deserialize_batches(value: bytes) -> list[pa.RecordBatch]:
    """Inverse of :func:`serialize_batch` for one stored blob."""
    reader = pa.ipc.open_stream(pa.BufferReader(value))
    return cast("list[pa.RecordBatch]", reader.read_all().to_batches())


def input_schema_of(params: Any) -> pa.Schema:
    """Input schema from a process/finalize params object."""
    schema = params.init_call.bind_call.input_schema
    assert schema is not None
    return schema


class SinkBuffer[TArgs, TState](TableBufferingFunction[TArgs, TState]):
    """Single-bucket sink/combine: buffer every input batch under one key.

    Subclasses implement ``on_bind``, ``initial_finalize_state``, and
    ``finalize`` (calling ``buffered_frame(params)`` to get the full input as a
    ``pandas.DataFrame``).
    """

    @classmethod
    def process(cls, batch: pa.RecordBatch, params: TableBufferingParams[TArgs]) -> bytes:
        """Sink one input batch to execution-scoped storage under the single bucket.

        Args:
            batch: One input RecordBatch from the upstream relation.
            params: The buffering-function call context.

        Returns:
            The execution id, used as this process call's state id.
        """
        if batch.num_rows:
            params.storage.state_append(_DATA_KEY, b"", serialize_batch(batch))
        return params.execution_id

    @classmethod
    def combine(cls, state_ids: list[bytes], params: TableBufferingParams[TArgs]) -> list[bytes]:
        """Collapse every process state id to the single finalize bucket.

        Args:
            state_ids: The state ids produced by ``process`` across all batches.
            params: The buffering-function call context.

        Returns:
            A one-element list (the execution id) so finalize runs in one bucket.
        """
        return [params.execution_id]

    @classmethod
    def buffered_frame(cls, params: TableBufferingParams[TArgs]) -> pd.DataFrame:
        """Reassemble all sunk batches into a single pandas DataFrame.

        Args:
            params: The buffering-function call context (carries storage + the
                bound input schema).

        Returns:
            The buffered relation as a single ``pandas.DataFrame``. An empty
            (zero-row) frame -- with the right column names -- when no rows were
            sunk, so finalize can apply uniform empty-input handling.
        """
        input_schema = input_schema_of(params)
        batches: list[pa.RecordBatch] = []
        for _sid, value in params.storage.state_log_scan(_DATA_KEY, b""):
            batches.extend(deserialize_batches(value))
        if not batches:
            return cast("pd.DataFrame", pa.Table.from_batches([], schema=input_schema).to_pandas())
        return cast("pd.DataFrame", pa.Table.from_batches(batches, schema=input_schema).to_pandas())
