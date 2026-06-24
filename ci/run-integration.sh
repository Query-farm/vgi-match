#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Run this repo's sqllogictest suite (test/sql/*.test) against the vgi-match
# VGI worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community `vgi` extension — no C++ build from source. See ci/README.md.
#
# The SAME suite is exercised over three VGI transports, selected by $TRANSPORT.
# The vgi extension picks the transport from the LOCATION string the .test files
# ATTACH (`${VGI_MATCH_WORKER}`):
#
#   subprocess : a bare stdio command (`.venv/bin/vgi-match`) — the extension
#                spawns the worker per query and talks Arrow IPC over
#                stdin/stdout. Default; current behavior.
#   http       : the worker is started out-of-band in `--http` mode on an auto
#                port; LOCATION becomes `http://127.0.0.1:<port>`.
#   unix       : the worker is started out-of-band on an AF_UNIX socket;
#                LOCATION becomes `unix://<sock>`.
#
# Required environment:
#   HAYBARN_UNITTEST     path to the haybarn-unittest binary
#   VGI_MATCH_WORKER     the stdio command that runs the worker (the installed
#                        `vgi-match` console script in the project venv). Used
#                        directly as the LOCATION for subprocess, and as the
#                        process this script boots for http/unix.
# Optional:
#   TRANSPORT            subprocess | http | unix (default: subprocess)
#   STAGE                scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"
: "${VGI_MATCH_WORKER:?worker LOCATION (stdio command running the worker)}"

TRANSPORT="${TRANSPORT:-subprocess}"
case "$TRANSPORT" in
  subprocess|http|unix) ;;
  *) echo "ERROR: unknown TRANSPORT='$TRANSPORT' (expected subprocess|http|unix)" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"

# The stdio command the subprocess transport ATTACHes to is also the command we
# launch out-of-band for http/unix. Capture it before we possibly overwrite
# VGI_MATCH_WORKER with a URL.
WORKER_CMD="$VGI_MATCH_WORKER"

echo "Staging preprocessed tests into $STAGE ..."
mkdir -p "$STAGE/test/sql"
for f in "$REPO"/test/sql/*.test; do
  awk -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/$(basename "$f")"
done

# ---------------------------------------------------------------------------
# Per-transport: resolve VGI_MATCH_WORKER (the LOCATION) and, for the
# out-of-band transports, boot the worker server + arrange trap-cleanup.
# ---------------------------------------------------------------------------
SERVER_PID=""
SOCK=""
PORT_FILE=""

cleanup() {
  # Preserve the script's exit status: this runs on EXIT, so its own last
  # command must not clobber the real exit code (a bare `[ -n "$x" ]` that is
  # false returns 1 and would turn a green run red under `set -e`).
  local rc=$?
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  if [ -n "$SOCK" ]; then rm -f "$SOCK"; fi
  if [ -n "$PORT_FILE" ]; then rm -f "$PORT_FILE"; fi
  return "$rc"
}
trap cleanup EXIT

case "$TRANSPORT" in
  subprocess)
    echo "Transport: subprocess/stdio — VGI_MATCH_WORKER=$VGI_MATCH_WORKER"
    ;;

  http)
    # The vgi extension's HTTP transport is implemented on top of DuckDB's
    # httpfs extension, so an `http://` ATTACH binds with
    #   "Binder Error: VGI HTTP transport requires the httpfs extension."
    # unless httpfs is loaded first. (The haybarn sqllogictest runner's default
    # skip list swallows any error containing "HTTP", so without this the whole
    # suite would silently SKIP rather than fail — a fake pass; see the run-step
    # guard below.) The .test files are transport-agnostic; inject a signed
    # `INSTALL httpfs FROM core; LOAD httpfs;` right after the `LOAD vgi;` in
    # each staged file, so httpfs is present only when we run over HTTP.
    echo "Transport http: injecting 'LOAD httpfs' into staged tests (required for the worker HTTP RPC) ..."
    for sf in "$STAGE"/test/sql/*.test; do
      awk '
        { print }
        /^LOAD[ \t]+vgi[ \t]*;[ \t]*$/ {
          print "";
          print "statement ok";
          print "INSTALL httpfs FROM core;";
          print "";
          print "statement ok";
          print "LOAD httpfs;";
        }
      ' "$sf" > "$sf.tmp" && mv "$sf.tmp" "$sf"
    done

    # Boot the worker in HTTP mode on an auto-selected port. The worker writes
    # the chosen port to --port-file atomically, so we watch for the file to
    # appear rather than parsing stdout. HTTP mode needs the `http` extra
    # (waitress); the integration job installs it via `uv sync --extra http`.
    # cwd = STAGE so any relative path the worker resolves matches the runner.
    PORT_FILE="$(mktemp -u "${TMPDIR:-/tmp}/match-port.XXXXXX")"
    LOG_FILE="${TMPDIR:-/tmp}/match-http-server.log"
    echo "Starting HTTP worker: $WORKER_CMD --http --port 0 --port-file $PORT_FILE (cwd=$STAGE)"
    # shellcheck disable=SC2086
    ( cd "$STAGE" && exec $WORKER_CMD --http --port 0 --port-file "$PORT_FILE" ) > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!

    PORT=""
    for _ in $(seq 1 120); do
      if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: HTTP worker exited before reporting a port. Log:" >&2
        cat "$LOG_FILE" >&2
        exit 1
      fi
      if [ -s "$PORT_FILE" ]; then
        PORT="$(tr -d '[:space:]' < "$PORT_FILE")"
        [ -n "$PORT" ] && break
      fi
      sleep 0.5
    done
    if [ -z "$PORT" ]; then
      echo "ERROR: timed out waiting for HTTP worker port-file. Log:" >&2
      cat "$LOG_FILE" >&2
      exit 1
    fi
    # The LOCATION must be the bare scheme://host:port with NO path suffix; the
    # extension POSTs each RPC method at <LOCATION>/<method>.
    export VGI_MATCH_WORKER="http://127.0.0.1:$PORT"
    echo "HTTP worker ready on $VGI_MATCH_WORKER (pid $SERVER_PID)"
    ;;

  unix)
    # Boot the worker bound to an AF_UNIX socket; it prints `UNIX:<path>` once
    # bound. We poll for the socket file to appear. cwd = STAGE (see http note).
    SOCK="${TMPDIR:-/tmp}/match-$$.sock"
    rm -f "$SOCK"
    LOG_FILE="${TMPDIR:-/tmp}/match-unix-server.log"
    echo "Starting unix worker: $WORKER_CMD --unix $SOCK (cwd=$STAGE)"
    # shellcheck disable=SC2086
    ( cd "$STAGE" && exec $WORKER_CMD --unix "$SOCK" ) > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!

    READY=""
    for _ in $(seq 1 120); do
      if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: unix worker exited before binding the socket. Log:" >&2
        cat "$LOG_FILE" >&2
        exit 1
      fi
      if [ -S "$SOCK" ]; then
        READY=1
        break
      fi
      sleep 0.5
    done
    if [ -z "$READY" ]; then
      echo "ERROR: timed out waiting for unix worker socket. Log:" >&2
      cat "$LOG_FILE" >&2
      exit 1
    fi
    export VGI_MATCH_WORKER="unix://$SOCK"
    echo "unix worker ready on $VGI_MATCH_WORKER (pid $SERVER_PID)"
    ;;
esac

cd "$STAGE"

# Warm the extension cache once: vgi from the signed community channel. A miss
# here is only a warning — the per-test INSTALL/LOAD (injected by
# preprocess-require.awk) is what actually gates each file.
echo "Warming the extension cache (vgi from community) ..."
mkdir -p "$STAGE/test"
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
INSTALL vgi FROM community;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

# Run the whole suite in one invocation, capturing the runner's native
# sqllogictest report so we can both stream it AND guard against a silent skip.
#
# IMPORTANT: the DuckDB/Haybarn sqllogictest runner SKIPS (not fails, exit 0) a
# test whose error message matches a built-in network-error allowlist that
# includes the substring "HTTP". So a broken HTTP transport would otherwise show
# "All tests were skipped" and the job would go GREEN having run nothing — a
# fake pass. We detect that and fail explicitly. A real run prints
# "All tests passed (N assertions ...)".
echo "Running suite (transport: $TRANSPORT, worker: $VGI_MATCH_WORKER) ..."
RUN_LOG="$STAGE/run.log"
set +e
"$HAYBARN_UNITTEST" "test/sql/*" 2>&1 | tee "$RUN_LOG"
RUN_RC="${PIPESTATUS[0]}"
set -e

if [ "$RUN_RC" -ne 0 ]; then
  echo "ERROR: suite failed (transport: $TRANSPORT, rc=$RUN_RC)" >&2
  exit "$RUN_RC"
fi

# Guard against the silent-skip fake-pass (see comment above). If every test was
# skipped — and none ran — treat it as a failure for this transport, surfacing
# the skip reason the runner reported.
if grep -q 'All tests were skipped' "$RUN_LOG"; then
  echo "ERROR: every test was SKIPPED on transport '$TRANSPORT' (the runner's" >&2
  echo "       built-in network-error skip swallowed the real error). This is" >&2
  echo "       NOT a pass. Skip reason reported by the runner:" >&2
  grep -A3 'Skipped tests for the following reasons' "$RUN_LOG" >&2 || true
  exit 1
fi
