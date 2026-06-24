"""VGI worker exposing probabilistic entity resolution (Splink) to DuckDB/SQL.

Assembles the table function(s) in ``vgi_match`` into a single ``match`` catalog
and provides the process entry point. The repo-root ``match_worker.py`` is a
thin shim over this module for ``uv run``; installed users get the ``vgi-match``
console script, which calls ``main`` here.

    ATTACH 'match' (TYPE vgi, LOCATION 'uv run match_worker.py');
    SELECT * FROM match.match_resolve((SELECT * FROM customers),
                                      columns := 'first_name,last_name,email');
"""

from __future__ import annotations

import contextlib
import logging
import sys
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema

from vgi_match.tables import TABLE_FUNCTIONS

_FUNCTIONS: list[type] = [*TABLE_FUNCTIONS]

_CATALOG_DESCRIPTION_LLM = (
    "Probabilistic entity resolution / record linkage / dedup over a SQL relation. "
    "Pass a relation of records, name the comparison columns, and get the input rows "
    "back unchanged plus a cluster_id (rows sharing it are the same real-world entity) "
    "and a match_probability. Backed by Splink's Fellegi-Sunter model with fuzzy "
    "(Jaro-Winkler / Levenshtein) comparisons. Use to dedup customers, contacts, "
    "products, or any list with messy duplicates, and to link records that refer to "
    "the same entity. One whole-relation table function: match_resolve."
)
_CATALOG_DESCRIPTION_MD = (
    "# match\n\n"
    "Probabilistic **entity resolution / record linkage / deduplication** for DuckDB, "
    "backed by [Splink](https://github.com/moj-analytical-services/splink) "
    "(Fellegi-Sunter model with fuzzy comparisons).\n\n"
    "Table function: `match_resolve(relation, columns := '...', threshold := 0.5, "
    "train := false)` returns the input rows unchanged plus `cluster_id` and "
    "`match_probability`."
)
_SCHEMA_DESCRIPTION_LLM = (
    "Entity-resolution functions: cluster a relation's rows into resolved entities "
    "(dedup / record linkage) on chosen comparison columns and return each row's "
    "cluster_id and match_probability."
)
_SCHEMA_DESCRIPTION_MD = "Probabilistic entity-resolution / record-linkage / dedup functions over Apache Arrow."

_MATCH_CATALOG = Catalog(
    name="match",
    default_schema="main",
    comment="Probabilistic entity resolution / record linkage / dedup (Splink) for SQL.",
    source_url="https://github.com/Query-farm/vgi-match",
    tags={
        "vgi.description_llm": _CATALOG_DESCRIPTION_LLM,
        "vgi.description_md": _CATALOG_DESCRIPTION_MD,
        "vgi.author": "Query.Farm",
        "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
        "vgi.license": "MIT",
        "vgi.support_contact": "https://github.com/Query-farm/vgi-match/issues",
        "vgi.support_policy_url": "https://github.com/Query-farm/vgi-match/blob/main/README.md",
    },
    schemas=[
        Schema(
            name="main",
            comment="Probabilistic entity resolution / record linkage / dedup (Splink) for SQL",
            tags={
                "vgi.description_llm": _SCHEMA_DESCRIPTION_LLM,
                "vgi.description_md": _SCHEMA_DESCRIPTION_MD,
            },
            functions=list(_FUNCTIONS),
        ),
    ],
)


def _warm_up() -> None:
    """Import Splink once at startup so the first query doesn't stall.

    Splink (and its DuckDB / igraph / sqlglot stack) is multi-hundred-millisecond
    to import. Doing it lazily means the *first* match_resolve of every ATTACH
    pays that cost inline -- a window in which a worker-pool teardown SIGTERM (or
    a loaded host) can kill the run mid-assertion and record a spurious E2E
    failure. Importing here moves the cost to process spawn, before any query.
    Best-effort: an import failure here is not fatal (the function will raise its
    own actionable error if actually invoked), so a worker still starts cleanly.
    """
    logging.getLogger("splink").setLevel(logging.ERROR)
    with contextlib.suppress(Exception):
        import splink  # noqa: F401, PLC0415  (warm the heavy import)
        import splink.comparison_library  # noqa: F401, PLC0415


class MatchWorker(Worker):
    """Worker process hosting the ``match`` catalog."""

    catalog = _MATCH_CATALOG

    def run(self, otel_config: Any = None) -> None:
        """Warm the Splink import, then serve."""
        _warm_up()
        super().run(otel_config=otel_config)


def main() -> None:
    """Run the worker (stdio by default; pass ``--http`` for the HTTP server)."""
    MatchWorker.main()


def main_http() -> None:
    """Run the worker over HTTP (injects ``--http`` into the worker CLI)."""
    argv = sys.argv[1:]
    if "--http" not in argv:
        argv = ["--http", *argv]
    sys.argv = [sys.argv[0], *argv]
    MatchWorker.main()
