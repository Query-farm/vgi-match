# Entity Resolution notebooks

[01 · Audit duplicate groups before trusting a score](01_audit_duplicate_groups.ipynb)
uses the worker's seven synthetic customer records, holds their authored labels
outside the matching input, and compares pair membership and evaluation outcomes.
The threshold experiment distinguishes missed duplicates from the worker's singleton
score convention. The shipped sample is a teaching fixture, not independent validation.

[Open in Colab](https://colab.research.google.com/github/Query-farm/vgi-match/blob/main/notebooks/01_audit_duplicate_groups.ipynb)
and run all. The bootstrap installs only the base
[shared package v0.3.9](https://github.com/Query-farm/vgi-example-notebooks/tree/v0.3.9).
It supplies the proper published catalog title/subtitle, branding and SVG presentation.
Local publication adds `[runner]`; no local helper or fixture files are needed.
GitHub renders linked SVG charts; Jupyter and Colab reruns show native SVG.

## Run locally

Install Git and [uv](https://docs.astral.sh/uv/). From this directory:

```sh
uv sync --locked
uv run --locked vgi-notebooks 01_audit_duplicate_groups.ipynb --timeout 300
```

For interactive use, select `notebooks/.venv/bin/python`, restart and run all.
The runner creates its own temporary kernel specification. Parameters, connection
and analytical SQL remain visible; setup, plotting and cleanup Python are folded.
The notebook runs independently of the repository source from a single notebook file.

The first run needs network access for Python dependencies, the VGI extension and
the released worker. Later runs use caches. The SQL connection starts with an empty
temporary secret directory and persistent secrets disabled. No account, credentials,
external customer data or messages are involved.

## Published worker connection

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'match' (TYPE vgi,
 LOCATION 'uvx --python 3.14 --from git+https://github.com/Query-farm/vgi-match@v0.4.0 vgi-match');
```

The short command pins worker source to public release v0.4.0, resolving to commit
`0e1dca756785ef40d1632187b20df96838e8a648`. It uses an isolated Python 3.14 worker
environment; `uv` provisions the interpreter when needed. There are no dependency
overrides or local checkout paths in the launch. The notebook kernel stays separate.

The tested release resolver selected vgi-python 0.51.1, vgi-rpc 0.53.1, Splink 5.0.0,
DuckDB 2.0.0, pandas 3.1.1, PyArrow 26.0.1 and NumPy 2.5.4. The worker tag pins its
source; transitive packages follow its published dependency declarations rather
than a notebook-imposed lock. The separate notebook lock pins Haybarn 2.0.0rc11 and
presentation dependencies. Review dependency changes and rerun both thresholds when
upgrading or refreshing the worker environment; no engine build is needed.

## Focused threshold experiment

```sh
uv run --locked vgi-notebooks 01_audit_duplicate_groups.ipynb \
  --parameters '{"THRESHOLD":0.9999}' --output-dir artifacts/strict --timeout 300
```

At `0.5`, the saved run finds four clusters and recovers all four authored duplicate
pairs. At `0.9999`, all seven rows are singletons: all four duplicate pairs are missed,
with no false groupings and undefined precision.

The stricter cutoff keeps all seven records and authored labels
fixed while changing retained links. Compare pair recall, cluster count and singleton
scores. Precision is SQL NULL if no pairs are grouped. Overrides require a separate
destination, unknown parameters fail, and failed execution preserves successful
saved defaults. Generated logs, review PNGs and experiment copies are ignored.

The notebook accepts finite `0 < THRESHOLD < 1`. The tested Splink runtime generates
an invalid infinite SQL cutoff at exactly `1.0`; use `0.9999` for this experiment.
Training stays explicitly false. This example does not select a production threshold.

## Interpretation and limits

`sample_customers` supplies invented records with reserved example-domain emails.
We materialize it once, inspect the planted group sizes, project customer features
into `customers`, and keep `expected_entity` in a separate `fixture_truth` table.
The truth label never enters `match_resolve`. Adding an `R` prefix to the supplied
integer row IDs is only a display convention. The worker project authored both the
sample and labels; agreement is not independent evidence of model quality.

Exact equality on at least one comparison field generates candidate pairs; fuzzy
comparisons then score them. Rows differing in every comparison field can be missed
before scoring. Retained links form connected components, so two rows can be grouped
through a third without a direct above-threshold link between them. Evaluation uses
all 21 unordered pairs, including pairs excluded by blocking.

`match_probability` is the strongest retained pair score touching a linked row.
Singleton/unscored rows receive **1.0**. It is not a cluster-level confidence or a
probability of uniqueness. The notebook preserves the raw value and labels its
context. A stricter cutoff can create more singleton scores of 1.0 while missing
more duplicate pairs. Undefined precision stays distinct from zero or perfect precision.

The worker buffers the full input and uses its own DuckDB backend. This is not a
streaming or scale benchmark. Seven synthetic rows do not calibrate probabilities,
justify model training, or establish performance on real customer data.

## Coverage and verification

[coverage.json](coverage.json) maps both public logical names to meaningful visible
executed SQL: **2/2 released and current interfaces**. Complete native function,
table and view exports are retained. The built-in sample is a view and is included
in the denominator. Its group inventory guides the labels withheld from matching;
the resolver supplies the cluster/score results used in every pair audit and chart.

The notebook executes v0.4.0. Current public baseline
`58d389117fc075fb644c1eb724c0b16c52311fad` has the same two interfaces and identical
sample rows, verified through its existing installed worker. Reviewed current-interface
coverage is distinct from exact-current notebook execution. Root source and dependency
locks remain unchanged.

Default execution, a detached strict-threshold experiment, chart inspection and
runner/parameter guards are recorded in the rollout report. A fresh preloaded
Python 3.12 kernel tests base-only bootstrap compatibility without installing Splink,
worker VGI packages, worker DuckDB or the local runner into that kernel. It is a local
compatibility test, not a claim of a manual hosted-Colab session.

The existing green [source CI baseline](https://github.com/Query-farm/vgi-match/actions/runs/43573529768)
is reused for this notebook-only change. No source rebuild, repeated worker test
suite or container publication is required. Ten SQL checks audit row preservation,
score bounds, pair completeness and metric denominators without requiring perfect
matching at every threshold.

The worker and Splink are MIT licensed; Splink's runtime dependency python-igraph
is GPL-2.0. Official unchanged branding links to [Query.Farm](https://query.farm/)
and [VGI](https://query.farm/vgi/).
