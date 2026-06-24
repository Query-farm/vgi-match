# CI: the vgi-match worker integration suite

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs the unit tests
and this repo's sqllogictest suite (`test/sql/*.test`) against the vgi-match
VGI worker through the **real DuckDB `vgi` extension** on every push / PR.

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source, CI drives a
**prebuilt** standalone `haybarn-unittest` (the DuckDB/Haybarn sqllogictest
runner, published in Haybarn's releases) and installs the **signed** `vgi`
extension from the Haybarn community channel:

1. **Install the worker** — `uv sync --frozen` into a venv. `.venv/bin/vgi-match`
   is the installed console script (`vgi_match.worker:main`) the extension can
   spawn over stdio.
2. **Download the runner** — the matching `haybarn_unittest-*` asset per
   platform from the latest Haybarn release.
3. **Preprocess** — the standalone runner links none of the extensions the
   tests gate on, so [`preprocess-require.awk`](preprocess-require.awk) rewrites
   each `require <ext>` into an explicit signed `INSTALL <ext> FROM
   {community,core}; LOAD <ext>;`. These tests skip `require vgi` (haybarn
   silently SKIPs it) and `LOAD vgi;` directly, so the awk also injects an
   `INSTALL vgi FROM community;` right before each bare `LOAD vgi;`. `require-env`
   and everything else pass through untouched.
4. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed
   tree, points `VGI_MATCH_WORKER` at `.venv/bin/vgi-match`, warms the
   extension cache once, then runs the suite in a single `haybarn-unittest`
   invocation. Any failed assertion exits non-zero and fails the job.

## Three transports (subprocess / http / unix)

The same suite runs over every VGI transport. The vgi extension picks the
transport from the ATTACH LOCATION string `run-integration.sh` builds per the
`TRANSPORT` env var (`subprocess` default | `http` | `unix`):

- **subprocess** — `VGI_MATCH_WORKER=.venv/bin/vgi-match`; the extension spawns
  the worker per query over stdin/stdout (current behavior).
- **http** — the script boots `vgi-match --http --port 0 --port-file <f>` (cwd =
  the stage dir), polls the port-file, and sets
  `VGI_MATCH_WORKER=http://127.0.0.1:<port>`. The HTTP transport runs the
  worker-RPC over DuckDB's httpfs, so the script injects
  `INSTALL httpfs FROM core; LOAD httpfs;` after each `LOAD vgi;` in the staged
  tests (http leg only) — without it the ATTACH errors with "VGI HTTP transport
  requires the httpfs extension", which the runner silently SKIPs (a fake pass
  the run-step guard catches). HTTP needs the `vgi-python[http]` extra
  (waitress) — installed via `uv sync --extra http`.
- **unix** — the script boots `vgi-match --unix <sock>` (cwd = the stage dir),
  polls for the socket, and sets `VGI_MATCH_WORKER=unix://<sock>`.

The CI `integration` job is a matrix of `transport × os`. For http/unix the
script boots the worker out-of-band and trap-kills it on exit; the run step
fails the leg if the runner reports "All tests were skipped" (the silent-skip
guard).

## Run it locally

```bash
uv sync --python 3.13                       # install the worker + deps
# point HAYBARN_UNITTEST at a haybarn-unittest binary (or a local DuckDB
# `unittest` built with the vgi extension), and the worker at the stdio command:
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
VGI_MATCH_WORKER="$PWD/.venv/bin/vgi-match" \
  ci/run-integration.sh
```

Or use the Makefile target `make test-sql`, which installs `haybarn-unittest`
as a uv tool and points the worker at `.venv/bin/vgi-match`.
