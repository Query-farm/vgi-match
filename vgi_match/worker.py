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
from vgi.catalog import Catalog, Schema, View

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
    "A typical call passes your rows as a parenthesised subquery and names the "
    "comparison columns -- `match_resolve(relation, columns := "
    "'first_name,last_name,email')` -- then orders or groups the result by "
    "`cluster_id` to inspect each resolved entity or to count how many records "
    "collapsed into it. Fully runnable, coverage-checked queries live in the "
    "function's own example set (see `match.main.match_resolve`) and in the "
    "`main` schema examples.\n\n"
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
    "Call `match_resolve(relation, columns := 'first_name,last_name')`, passing your "
    "rows as a parenthesised subquery, then group by `cluster_id` to count records "
    "per resolved entity. Runnable, coverage-checked queries are attached to the "
    "`match.main.match_resolve` function and to this schema's example set.\n\n"
    "## Notes\n\n"
    "Comparison columns are passed through to the output too; every non-comparison "
    "column rides along unchanged."
)
_SCHEMA_CATEGORIES = (
    '[{"name": "Entity Resolution", "description": "Cluster a relation\'s rows into '
    "resolved real-world entities -- deduplication and record linkage -- with "
    "probabilistic fuzzy matching, returning each row's cluster_id and match_probability.\"}]"
)

# VGI152: analyst tasks so `vgi-lint simulate` can measure how well an agent uses
# this worker. Each reference_sql is a deterministic scalar over match_resolve of
# an inline, clearly-separable VALUES relation (identical last_name+email for the
# duplicate pairs, distinct singletons), so the default deterministic model links
# the obvious dups stably across runs. Graded value-only / unordered.
_AGENT_TEST_TASKS = (
    "["
    '{"name": "count-resolved-entities", '
    '"prompt": "Deduplicate this customer list on first name, last name and email, and '
    "return how many distinct real-world customers there are. The records are "
    "(John, Smith, john@x.com), (Jon, Smith, john@x.com), (Jane, Doe, jane@y.com), "
    '(Jayne, Doe, jane@y.com), (Bob, Ng, bob@z.com).", '
    '"reference_sql": "SELECT count(DISTINCT cluster_id) AS entities FROM '
    "match.main.match_resolve((SELECT * FROM (VALUES ('John','Smith','john@x.com'),"
    "('Jon','Smith','john@x.com'),('Jane','Doe','jane@y.com'),('Jayne','Doe','jane@y.com'),"
    "('Bob','Ng','bob@z.com')) AS t(first_name,last_name,email)), "
    "columns := 'first_name,last_name,email')\", "
    '"unordered": true, "ignore_column_names": true}, '
    '{"name": "largest-duplicate-group", '
    '"prompt": "Resolve the same five customer records ((John, Smith, john@x.com), '
    "(Jon, Smith, john@x.com), (Jane, Doe, jane@y.com), (Jayne, Doe, jane@y.com), "
    "(Bob, Ng, bob@z.com)) on first name, last name and email, and report how many "
    'records are in the largest duplicate group.", '
    '"reference_sql": "SELECT max(n) AS largest_group FROM (SELECT cluster_id, '
    "count(*) AS n FROM match.main.match_resolve((SELECT * FROM (VALUES "
    "('John','Smith','john@x.com'),('Jon','Smith','john@x.com'),('Jane','Doe','jane@y.com'),"
    "('Jayne','Doe','jane@y.com'),('Bob','Ng','bob@z.com')) AS t(first_name,last_name,email)), "
    "columns := 'first_name,last_name,email') GROUP BY cluster_id)\", "
    '"unordered": true, "ignore_column_names": true}, '
    '{"name": "count-duplicate-groups", '
    '"prompt": "For the same five customer records ((John, Smith, john@x.com), '
    "(Jon, Smith, john@x.com), (Jane, Doe, jane@y.com), (Jayne, Doe, jane@y.com), "
    "(Bob, Ng, bob@z.com)), resolve entities on first name, last name and email and "
    'count how many resolved entities contain more than one record (duplicate groups).", '
    '"reference_sql": "SELECT count(*) AS duplicate_groups FROM (SELECT cluster_id FROM '
    "match.main.match_resolve((SELECT * FROM (VALUES ('John','Smith','john@x.com'),"
    "('Jon','Smith','john@x.com'),('Jane','Doe','jane@y.com'),('Jayne','Doe','jane@y.com'),"
    "('Bob','Ng','bob@z.com')) AS t(first_name,last_name,email)), "
    "columns := 'first_name,last_name,email') GROUP BY cluster_id HAVING count(*) > 1)\", "
    '"unordered": true, "ignore_column_names": true}, '
    '{"name": "browse-sample-customers", '
    '"prompt": "This worker ships a built-in demo relation of customer records with '
    "planted duplicates. Using that sample data, how many records have the surname "
    'Smith?", '
    '"reference_sql": "SELECT count(*) AS n FROM match.main.sample_customers '
    "WHERE last_name = 'Smith'\", "
    '"unordered": true, "ignore_column_names": true}'
    "]"
)

_SCHEMA_EXAMPLE_QUERIES = (
    '[{"description": "Resolve three records on name + email and inspect the entity '
    'each row was assigned to, grouped by cluster.", '
    '"sql": "SELECT first_name, last_name, email, cluster_id FROM '
    "match.main.match_resolve((SELECT * FROM (VALUES ('John','Smith','j@x.com'),"
    "('Jon','Smith','j@x.com'),('Jane','Doe','jane@y.com')) AS t(first_name,last_name,email)), "
    "columns := 'first_name,last_name,email') ORDER BY cluster_id, first_name\"}, "
    '{"description": "Count how many records collapsed into each resolved entity, '
    'largest duplicate group first.", '
    '"sql": "SELECT cluster_id, count(*) AS n FROM match.main.match_resolve('
    "(SELECT * FROM (VALUES ('Ann','Lee'),('Anne','Lee'),('Bob','Ng')) AS t(first_name,last_name)), "
    "columns := 'first_name,last_name') GROUP BY cluster_id ORDER BY n DESC\"}]"
)

# VGI146: a browsable, credential-free demo relation so an agent can SEE the
# shape match_resolve expects before it guesses arguments. VALUES-backed (no
# network, no backing store) so it scans instantly and clears VGI911. It is the
# canonical planted-duplicate fixture from tests/synthetic.py: three "Smith"
# variants and two "Doe" variants are each ONE real person; the two "Jones" rows
# are DIFFERENT people who merely share a surname. `expected_entity` is the
# ground-truth label a good resolver should reproduce.
_SAMPLE_VIEW_SQL = (
    "SELECT * FROM (VALUES "
    "(1, 'John',   'Smith', 'jsmith@example.com',   'New York',      'Smith, John'), "
    "(2, 'Jon',    'Smith', 'jsmith@example.com',   'New York',      'Smith, John'), "
    "(3, 'Johnny', 'Smith', 'jsmith@example.com',   'New York',      'Smith, John'), "
    "(4, 'Jane',   'Doe',   'jane.doe@example.com', 'Los Angeles',   'Doe, Jane'), "
    "(5, 'Janet',  'Doe',   'jane.doe@example.com', 'Los Angeles',   'Doe, Jane'), "
    "(6, 'Robert', 'Jones', 'rjones@example.com',   'San Francisco', 'Jones, Robert'), "
    "(7, 'Alice',  'Jones', 'alice.j@example.com',  'Seattle',       'Jones, Alice') "
    ") AS t(record_id, first_name, last_name, email, city, expected_entity)"
)

_SAMPLE_VIEW_EXAMPLES = (
    '[{"description": "Count the sample records that belong to each ground-truth '
    'entity -- the planted duplicate groups match_resolve should reproduce.", '
    '"sql": "SELECT expected_entity, count(*) AS records FROM '
    "match.main.sample_customers GROUP BY expected_entity "
    'ORDER BY records DESC, expected_entity"}, '
    '{"description": "Inspect just the messy Smith records (three spelling variants '
    'of one real person) to see the kind of duplication entity resolution targets.", '
    '"sql": "SELECT record_id, first_name, last_name, email FROM '
    "match.main.sample_customers WHERE last_name = 'Smith' ORDER BY record_id\"}]"
)

_SAMPLE_VIEW = View(
    name="sample_customers",
    definition=_SAMPLE_VIEW_SQL,
    comment=(
        "A small, browsable demo relation of customer records with planted duplicates "
        "(and a ground-truth expected_entity label) to try match_resolve against."
    ),
    column_comments={
        "record_id": (
            "Stable 1-based integer row identifier for the sample record (a dimensionless "
            "key, not a measured quantity or an ordinal to compute on)."
        ),
        "first_name": "Given name; deliberately varied across duplicates (John / Jon / Johnny).",
        "last_name": "Surname; note the two distinct Jones people who only share a surname.",
        "email": "Contact email; duplicates of one person share an email, distinct people do not.",
        "city": "City of the record, carried along as a non-comparison passthrough column.",
        "expected_entity": (
            "Ground-truth label: rows with the same value are the same real-world person, "
            "so a correct resolver should give them one cluster_id."
        ),
    },
    tags={
        "vgi.title": "Sample Customers (planted-duplicate demo)",
        # VGI123 classifying tags (bare keys) + VGI411 category coverage.
        "domain": "data-quality",
        "category": "entity-resolution",
        "topic": "sample-data",
        "vgi.category": "Entity Resolution",
        "vgi.keywords": (
            '["sample data", "demo", "fixture", "duplicates", "entity resolution", '
            '"customers", "record linkage", "ground truth"]'
        ),
        "vgi.doc_llm": (
            "A tiny, static demo relation of seven customer records with deliberately "
            "planted duplicates, exposed so an agent can browse a realistic messy input "
            "before calling match_resolve. Three Smith rows (John / Jon / Johnny, same "
            "email) are one real person; two Doe rows (Jane / Janet, same email) are "
            "another; the two Jones rows (Robert, Alice) are DIFFERENT people who merely "
            "share a surname. Columns: record_id, first_name, last_name, email, city, and "
            "expected_entity (the ground-truth grouping label). Query it directly to see "
            "the shape match_resolve consumes; feed first_name/last_name/email into "
            "match_resolve and cluster_id should reproduce expected_entity."
        ),
        "vgi.doc_md": (
            "# sample_customers\n\n"
            "A small, **browsable demo relation** of seven customer records with planted "
            "duplicates, so you can try entity resolution without supplying your own data.\n\n"
            "## The planted story\n\n"
            "- **Smith, John** -- rows 1-3 (`John` / `Jon` / `Johnny`, same email) are one "
            "real person with spelling variants.\n"
            "- **Doe, Jane** -- rows 4-5 (`Jane` / `Janet`, same email) are one real person.\n"
            "- **Jones** -- rows 6-7 (`Robert`, `Alice`) are *different* people who only "
            "share a surname -- the over-merge trap.\n\n"
            "## Columns\n\n"
            "`record_id`, `first_name`, `last_name`, `email`, `city`, and `expected_entity` "
            "(the ground-truth grouping label). Compare `expected_entity` against the "
            "`cluster_id` a resolver assigns to gauge quality.\n\n"
            "## Notes\n\n"
            "The relation is static and credential-free (VALUES-backed), so it scans "
            "instantly and is safe to use in docs, demos, and tests."
        ),
        "vgi.example_queries": _SAMPLE_VIEW_EXAMPLES,
    },
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
        "vgi.agent_test_tasks": _AGENT_TEST_TASKS,
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
                "vgi.categories": _SCHEMA_CATEGORIES,
                "vgi.doc_llm": _SCHEMA_DOC_LLM,
                "vgi.doc_md": _SCHEMA_DOC_MD,
                "vgi.example_queries": _SCHEMA_EXAMPLE_QUERIES,
            },
            views=[_SAMPLE_VIEW],
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
