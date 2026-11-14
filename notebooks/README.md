# VGI Match notebooks

[01 · Audit duplicate groups](01_audit_duplicate_groups.ipynb) resolves seven synthetic customer records, compares clusters against separately authored labels, and visualizes pair membership and evaluation outcomes. The threshold experiment distinguishes missed duplicates from the worker's singleton score convention.

## Run the saved example

Install [uv](https://docs.astral.sh/uv/) and Git. From this directory:

```sh
uv sync --frozen
uv run --frozen python execute.py
```

The first run needs network access to install the community VGI extension and fetch the public worker Git revision. `uv` can provision Python 3.14. Later runs use caches. No local worker build or parent checkout is required; copy this entire directory to run it elsewhere.

For interactive use, select this directory's `.venv/bin/python` as the notebook kernel in VS Code. Alternatively register a kernel:

```sh
uv run --frozen python -m ipykernel install --user --name vgi-match-notebooks --display-name 'Python (vgi-match notebooks)'
```

Restart the kernel and run all cells after editing parameters. Connection SQL remains expanded; imports, plotting, validation and cleanup source are folded. Saved outputs include both charts.

## Experiment

```sh
uv run --frozen python execute.py --parameters '{"THRESHOLD":0.9999}' --output-dir validation/strict
```

The saved default uses 0.5. The stricter cutoff keeps the seven input rows and authored labels fixed while changing retained links. Compare pair recall, cluster count and singleton scores; precision is NULL if no pairs are grouped. Overrides require a separate destination, unknown parameter names fail, and failed execution does not replace saved successful outputs. Experiment copies include the relative branding and runtime companions.

## Reproducible worker startup

The visible `ATTACH` uses public v0.4.0 commit `0e1dca756785ef40d1632187b20df96838e8a648` via `uvx`, with explicitly tested SDK and computational dependencies: vgi-python 0.51.0, vgi-rpc 0.53.1, Splink 5.0.0, DuckDB 2.0.0, pandas 3.1.1, PyArrow 26.0.1 and NumPy 2.5.4. The notebook uses a separate locked Haybarn 2.0.0rc11 environment. To upgrade, change the literal command deliberately and rerun both parameter settings before replacing saved outputs.

The record data and labels are invented; email addresses use reserved example domains. Truth labels are never passed into the matcher. No account or credential is required. Exact equality on at least one comparison field generates candidate pairs; fuzzy comparisons then score them. Connected components determine clusters, which are not verified identities. This tiny fixture does not calibrate probabilities or establish performance on real customer data.

`match_probability` is the strongest retained pair score for linked rows; singleton/unscored rows receive 1.0. It is not a cluster-level confidence or a probability of uniqueness. The notebook rejects endpoints and nonfinite values: the pinned Splink stack generates invalid infinite-cutoff SQL at threshold 1.0. Use 0.9999 for the strict experiment. Training remains explicitly false. The worker buffers the input relation and uses its own DuckDB backend; this example is not a streaming or scale benchmark.

The original source documentation sometimes describes automatic training and calibrated defaults; this notebook follows the tested explicit `train := false` behavior and makes no calibration claim. Original source, fixtures, dependencies and lockfile are unchanged.

## Validation

Default and detached-directory experiment execution, rendered-chart inspection, runner failure guards, notebook lint, repository pytest and SQL tests, HTTP/Unix SQL transports, Ruff, mypy, docstring lint and live source metadata checks are recorded in the rollout report. Source checks use the local installed worker; notebook execution exclusively uses the public pinned Git launcher.

Official Query.Farm Strata Sun wordmark and VGI artwork are copied unchanged from the approved notebook assets and linked at the top and bottom. Chart provenance distinguishes the synthetic records from returned worker results.
