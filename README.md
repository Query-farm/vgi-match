<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A <a href="https://query.farm">Query.Farm</a> VGI worker for DuckDB.</em></p>

# Probabilistic Entity Resolution & Record Linkage in DuckDB

> **vgi-match** · a [Query.Farm](https://query.farm) VGI worker · powered by Splink

A [VGI](https://query.farm) worker that brings **probabilistic entity
resolution** — record linkage and deduplication — to DuckDB/SQL, backed by
[Splink](https://github.com/moj-analytical-services/splink) (Ministry of
Justice, UK; **MIT** licensed). Hand it a relation of records and it returns the
same rows with an appended `cluster_id`: rows sharing a `cluster_id` are the
same real-world entity.

```sql
INSTALL vgi FROM community; LOAD vgi;
ATTACH 'match' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-match@v0.4.0 vgi-match');

-- Dedup customers on name + email. Rows sharing cluster_id are the same person.
SELECT * FROM match.match_resolve(
    (SELECT * FROM customers),
    columns := 'first_name,last_name,email'
) ORDER BY cluster_id;

-- How many distinct real entities are in the relation?
SELECT count(DISTINCT cluster_id)
FROM match.match_resolve((SELECT * FROM customers),
                         columns := 'first_name,last_name,email');
```

## Quick start

The worker isn't on PyPI; [uv](https://docs.astral.sh/uv/) runs it straight from a tagged release on GitHub. `uvx` builds an isolated environment on the first ATTACH and reuses it afterwards:

```sql
INSTALL vgi FROM community;
LOAD vgi;

ATTACH 'match' AS match (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-match@v0.4.0 vgi-match');

-- Resolve three records on name + email and inspect the entity each row was assigned to,
-- grouped by cluster.
SELECT first_name, last_name, email, cluster_id FROM match.main.match_resolve((SELECT * FROM (VALUES ('John','Smith','j@x.com'),('Jon','Smith','j@x.com'),('Jane','Doe','jane@y.com')) AS t(first_name,last_name,email)), columns := 'first_name,last_name,email') ORDER BY cluster_id, first_name;

-- Count how many records collapsed into each resolved entity, largest duplicate group
-- first.
SELECT cluster_id, count(*) AS n FROM match.main.match_resolve((SELECT * FROM (VALUES ('Ann','Lee'),('Anne','Lee'),('Bob','Ng')) AS t(first_name,last_name)), columns := 'first_name,last_name') GROUP BY cluster_id ORDER BY n DESC;

-- Build a golden record per entity from the built-in sample_customers demo: resolve on
-- name + email, then collapse each cluster to one row (the three Smith and two Doe
-- spellings each become one row; the two Jones stay separate).
SELECT min(record_id) AS golden_record_id, string_agg(DISTINCT first_name, ' / ' ORDER BY first_name) AS name_variants, any_value(last_name) AS last_name, any_value(email) AS email, any_value(city) AS city, count(*) AS source_records FROM match.main.match_resolve((SELECT record_id, first_name, last_name, email, city FROM match.main.sample_customers), columns := 'first_name,last_name,email') GROUP BY cluster_id ORDER BY golden_record_id;
```

Change `@v0.4.0` to move to a newer release.

### Other ways to run it

- **Docker** (the image bundles the worker and its dependencies):

  ```sql
  ATTACH 'match' AS match (TYPE vgi, LOCATION 'docker://ghcr.io/query-farm/vgi-match:0.4.0');
  ```

- **From a local checkout** (`git clone https://github.com/Query-farm/vgi-match`), point `LOCATION` at `uv run --project /path/to/vgi-match vgi-match`.

### Build from source

To run the worker from a source checkout or install its console scripts yourself:

```bash
# From a source checkout (stdio worker, exactly how DuckDB drives it after ATTACH):
uv run match_worker.py

# Or install the console script:
uv tool install git+https://github.com/Query-farm/vgi-match@v0.4.0    # provides `vgi-match` (stdio) and `vgi-match-http`
```

## Data flow: one relation in, the rows back plus a cluster id

`match_resolve` is a **table function** that consumes a *whole input relation* —
passed as a single `(SELECT ...)` subquery (the positional argument) — and emits
**the input rows unchanged**, with two appended columns:

| appended column | meaning |
|-----------------|---------|
| `cluster_id` (VARCHAR) | resolved-entity id — rows sharing it are the same entity |
| `match_probability` (DOUBLE) | strongest pairwise match probability linking the row into its cluster (`1.0` for a singleton) |

The linkage config is passed as **named arguments**:

| named arg | meaning |
|-----------|---------|
| `columns := 'c1,c2,...'` | comma-separated **comparison columns** to match records on (required) |
| `threshold := 0.9` | match-probability threshold in `[0, 1]` for linking pairs / forming clusters (default `0.5`) |

Every column **not** named in `columns` is passed straight through to the
output untouched — so you can `SELECT *` your relation in and keep all your
fields. The comparison columns are *also* passed through; they just additionally
drive the matching.

Because entity resolution is an **all-pairs whole-relation** problem (every
record is conceptually compared against every other), `match_resolve` is a
**buffering** (Sink+Source) function: it buffers all input batches, then runs
Splink once over the full relation.

## How the matching works (Splink / Fellegi-Sunter)

Splink makes all-pairs matching tractable with three ideas:

1. **Blocking** — only compare record pairs that agree on at least one blocking
   rule (here: agree on *any one* of the comparison columns), so we never
   materialize the full O(n²) cross product.
2. **Comparisons** — for each candidate pair, each comparison column is scored
   into agreement levels using string similarity. By default `vgi-match` uses a
   fuzzy **Jaro-Winkler** `NameComparison` for name-like columns (`first_name`,
   `last_name`, `surname`, …) and a fuzzy **Levenshtein** comparison
   (`≤1`, `≤2` edits, exact) for everything else — so typos and nicknames still
   match.
3. **The Fellegi-Sunter model** — a calibrated `match_probability` is computed
   per pair from per-level `m` (probability of that agreement level *given a true
   match*) and `u` (*given a non-match*) probabilities. Pairs above `threshold`
   are linked, and the **connected components** of the resulting match graph
   become the entity clusters.

### Training: off by default, unsupervised EM opt-in

By default **no training runs**: supply only `columns` and the worker builds a
model from Splink's library comparisons, whose calibrated default per-level `m`
/ `u` probabilities plus a fixed prior give **deterministic** match weights on a
relation of any size. `train := true` additionally runs Splink's **unsupervised
Expectation-Maximisation (EM)** on the buffered data (no labels required) to
refine `m` and the match rate. EM is unreliable on small inputs, so it is opt-in
and best-effort: if no EM pass can be trained, the default model is kept rather
than raising an error.

## Functions

| function | returns | does |
|----------|---------|------|
| `match_resolve(rel, columns, [threshold], [train])` | every input column + `cluster_id` + `match_probability`, one row per input row | dedup / resolve entities in `rel` |

## Memory characteristic (read this before you point it at 50M rows)

**Splink spins up its own DuckDB engine inside this worker process.** Rows
arrive over Arrow from the *caller's* DuckDB, are buffered into a single pandas
frame, and are then handed to Splink's DuckDB to run the linkage. So this is
**buffer-all-then-compute**: peak memory in the worker holds the whole input
relation, plus Splink's intermediate blocked-pairs tables. That is exactly the
right shape for the small/medium relations entity resolution usually runs on
(customer lists, registries, product catalogs), but it is **not** a streaming
operator — it is not free on very large relations. Block tightly (use selective
`columns`) and resolve in partitions if you must scale up.

## The serving / pre-trained-model path

v1 ships the **batch resolve** path: train-on-this-data-then-cluster, in one
call. The linkage core (`vgi_match/linkage.py`) is already designed so a
**pre-trained Splink settings** object can be loaded and reused instead of
training — `resolve(..., settings_json=...)` consumes a settings dict/JSON
as-is and skips EM. A single-record **`match_score` serving** function (score one
new record against a previously-trained model without re-clustering the whole
relation) is a **planned extension**, not yet exposed as a SQL function — see
`CLAUDE.md` for the honest gap list.

## Licensing

`vgi-match` is **MIT** licensed (see `LICENSE`). Its dependencies:

| dependency | license | role |
|------------|---------|------|
| [Splink](https://github.com/moj-analytical-services/splink) | **MIT** | the entity-resolution engine |
| [DuckDB](https://duckdb.org/) (Splink's backend + the caller) | **MIT** | runs the linkage SQL |
| [pandas](https://pandas.pydata.org/) | BSD-3-Clause | the buffered frame |
| [pyarrow](https://arrow.apache.org/) | Apache-2.0 | Arrow transport |
| Splink transitives ([sqlglot](https://github.com/tobymao/sqlglot) MIT, [igraph](https://igraph.org/) GPL-2.0, [jsonschema](https://github.com/python-jsonschema/jsonschema) MIT, numpy/altair BSD) | mixed permissive | SQL generation, graph clustering, validation |

> **Note on `igraph`:** Splink depends on `python-igraph`, which is **GPL-2.0**.
> It is used at *runtime* (for connected-components clustering) and is not
> statically linked into `vgi-match`, but downstream redistributors who bundle
> the full dependency closure should be aware of it. Everything `vgi-match`
> itself ships is MIT.

---

## Authorship & License

Written by [Query.Farm](https://query.farm).

Copyright 2026 Query Farm LLC - https://query.farm


## Notebooks

[Executed SQL and visualization walkthrough](notebooks/README.md): audit duplicate groups against separately authored labels and explore threshold sensitivity.
