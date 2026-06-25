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

_CATALOG_DOC_LLM = (
    "Probabilistic entity resolution / record linkage / dedup over a SQL relation. "
    "Pass a relation of records, name the comparison columns, and get the input rows "
    "back unchanged plus a cluster_id (rows sharing it are the same real-world entity) "
    "and a match_probability. Backed by Splink's Fellegi-Sunter model with fuzzy "
    "(Jaro-Winkler / Levenshtein) comparisons. Use to dedup customers, contacts, "
    "products, or any list with messy duplicates, and to link records that refer to "
    "the same entity. One whole-relation table function: match_resolve."
)
_CATALOG_DOC_MD = (
    "# match\n\n"
    "Probabilistic **entity resolution / record linkage / deduplication** for DuckDB, "
    "backed by [Splink](https://github.com/moj-analytical-services/splink) "
    "(a Fellegi-Sunter model with fuzzy comparisons).\n\n"
    "## What it does\n\n"
    "Give it a relation of messy records and the columns to compare on; it scores every "
    "candidate pair, links the ones above a probability threshold, and groups linked "
    "records into clusters that each represent one real-world entity.\n\n"
    "## Usage\n\n"
    "```sql\n"
    "SELECT * FROM match.match_resolve(\n"
    "  (SELECT * FROM customers),\n"
    "  columns := 'first_name,last_name,email'\n"
    ") ORDER BY cluster_id;\n"
    "```\n\n"
    "The single table function `match_resolve(relation, columns := '...', "
    "threshold := 0.5, train := false)` returns the input rows unchanged plus "
    "`cluster_id` (same entity = same id) and `match_probability`.\n\n"
    "## Notes\n\n"
    "- The default model is deterministic and robust on relations of any size; "
    "`train := true` opts into Splink's unsupervised EM (weak on tiny inputs).\n"
    "- This is a buffer-all-then-compute operator (entity resolution is an all-pairs "
    "problem), so block tightly for very large inputs."
)
_SCHEMA_DOC_LLM = (
    "Entity-resolution functions for SQL: cluster a relation's rows into resolved "
    "entities (dedup / record linkage) on chosen comparison columns and return each "
    "row's cluster_id and match_probability. The single table function "
    "match_resolve buffers the whole relation, runs Splink's Fellegi-Sunter model "
    "with fuzzy comparisons, and appends the cluster id and match probability to the "
    "passed-through input columns. Use it to deduplicate or link customer, contact, "
    "or product lists."
)
_SCHEMA_DOC_MD = (
    "# match.main\n\n"
    "The `main` schema holds the probabilistic **entity-resolution / record-linkage / "
    "deduplication** functions, operating over Apache Arrow relations.\n\n"
    "## Overview\n\n"
    "One table function, `match_resolve`, takes a whole input relation plus the "
    "comparison columns and returns the input rows unchanged with an appended "
    "`cluster_id` and `match_probability`.\n\n"
    "## Usage\n\n"
    "```sql\n"
    "SELECT cluster_id, count(*)\n"
    "FROM match.main.match_resolve((SELECT * FROM customers), columns := 'first_name,last_name')\n"
    "GROUP BY cluster_id;\n"
    "```\n\n"
    "## Notes\n\n"
    "Comparison columns are passed through to the output too; every non-comparison "
    "column rides along unchanged."
)
_SCHEMA_EXAMPLE_QUERIES = (
    "SELECT * FROM match.main.match_resolve("
    "(SELECT * FROM (VALUES ('John','Smith','j@x.com'),('Jon','Smith','j@x.com'),"
    "('Jane','Doe','jane@y.com')) AS t(first_name,last_name,email)), "
    "columns := 'first_name,last_name,email') ORDER BY cluster_id;\n"
    "SELECT cluster_id, count(*) AS n FROM match.main.match_resolve("
    "(SELECT * FROM (VALUES ('Ann','Lee'),('Anne','Lee'),('Bob','Ng')) AS t(first_name,last_name)), "
    "columns := 'first_name,last_name') GROUP BY cluster_id ORDER BY n DESC;"
)

_MATCH_CATALOG = Catalog(
    name="match",
    default_schema="main",
    comment="Probabilistic entity resolution / record linkage / dedup (Splink) for SQL.",
    source_url="https://github.com/Query-farm/vgi-match",
    tags={
        "vgi.title": "Entity Resolution & Record Linkage",
        "vgi.keywords": (
            '["entity resolution", "record linkage", "deduplication", "dedup", '
            '"fuzzy matching", "splink", "fellegi-sunter", "clustering", '
            '"customer matching", "duplicate detection"]'
        ),
        "vgi.doc_llm": _CATALOG_DOC_LLM,
        "vgi.doc_md": _CATALOG_DOC_MD,
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
                "vgi.title": "Match — main",
                "vgi.keywords": (
                    '["entity resolution", "record linkage", "dedup", "deduplication", '
                    '"match_resolve", "fuzzy matching", "clustering", "cluster_id", '
                    '"match probability", "splink"]'
                ),
                # VGI123 classifying tags (bare keys: domain/category/topic) for faceting.
                "domain": "data-quality",
                "category": "entity-resolution",
                "topic": "record-linkage",
                "vgi.doc_llm": _SCHEMA_DOC_LLM,
                "vgi.doc_md": _SCHEMA_DOC_MD,
                "vgi.example_queries": _SCHEMA_EXAMPLE_QUERIES,
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
