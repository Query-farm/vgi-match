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
    "# Match: Probabilistic Entity Resolution & Record Linkage in SQL\n\n"
    "![Splink logo](https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png)\n\n"
    "**Match brings fuzzy entity resolution, record linkage, and deduplication "
    "directly into DuckDB SQL** -- point it at a messy relation, name the columns "
    "to compare, and get back clusters of rows that refer to the same real-world "
    "person, customer, contact, or product.\n\n"
    "## What it does and who it's for\n\n"
    "Real-world data is full of duplicates that exact `GROUP BY` and `JOIN` can "
    "never catch: `John Smith` vs `Jon Smith`, `j.smith@x.com` vs `J.Smith@x.com`, "
    "typos, nicknames, and transpositions. Match solves this *probabilistic "
    "matching* problem in plain SQL. It is built for data engineers, analysts, and "
    "data-quality teams who need to deduplicate customer master data, consolidate "
    "CRM contacts, link records across systems, or clean a product catalog -- "
    "without exporting to a separate Python pipeline. You stay in DuckDB; Match "
    "does the fuzzy matching and clustering and hands you back the same rows plus "
    "an entity label.\n\n"
    "## How it works and the engine behind it\n\n"
    "Match is powered by [Splink](https://github.com/moj-analytical-services/splink), "
    "the open-source probabilistic record-linkage library from the UK Ministry of "
    "Justice. Under the hood it uses the **Fellegi-Sunter** statistical model with "
    "fuzzy string comparisons (Jaro-Winkler for name-like columns, Levenshtein edit "
    "distance for the rest) to estimate, for every candidate pair of records, the "
    "probability that they describe the same entity. Pairs scoring above a threshold "
    "are linked, and connected components of linked records collapse into entity "
    "clusters. The default model is deterministic and robust on relations of any "
    "size, while `train := true` opts into Splink's unsupervised EM estimation for "
    "larger datasets. See the [Splink documentation]"
    "(https://moj-analytical-services.github.io/splink/) and this "
    "[introduction to probabilistic linkage]"
    "(https://www.robinlinacre.com/intro_to_probabilistic_linkage/) for the theory.\n\n"
    "## SQL use cases and the function surface\n\n"
    "Match exposes a single, focused table function, "
    "`match_resolve(relation, columns := '...', threshold := 0.5, train := false)`. "
    "Pass a relation as a subquery and the comparison columns; it returns every "
    "input row unchanged plus two appended columns: `cluster_id` (rows sharing it "
    "are the same entity) and `match_probability` (confidence per row). Use it to "
    "deduplicate a customer or contact list, link records that refer to the same "
    "organization, or detect duplicate products before a migration.\n\n"
    "```sql\n"
    "SELECT * FROM match.match_resolve(\n"
    "  (SELECT * FROM customers),\n"
    "  columns := 'first_name,last_name,email'\n"
    ") ORDER BY cluster_id;\n"
    "```\n\n"
    "Count how many records collapsed into each resolved entity:\n\n"
    "```sql\n"
    "SELECT cluster_id, count(*) AS records\n"
    "FROM match.match_resolve((SELECT * FROM contacts), columns := 'name,phone')\n"
    "GROUP BY cluster_id ORDER BY records DESC;\n"
    "```\n\n"
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
