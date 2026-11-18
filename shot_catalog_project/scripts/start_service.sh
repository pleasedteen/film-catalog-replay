#!/usr/bin/env bash
#
# Start the full system on localhost:
#   1. the local paged log source (serves a bundled history with messy delivery)
#   2. the shot catalog query service, which also runs the sync loop against #1
#
# Usage: scripts/start_service.sh [history.jsonl] [db.duckdb]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PYLIBS="${PYLIBS:-/workspace/pylibs}"
export PYTHONPATH="$ROOT/src:$PYLIBS/lib/python3.11/site-packages"

HISTORY="${1:-$ROOT/fixtures/mixed_history.jsonl}"
DB="${2:-$ROOT/run/catalog.duckdb}"
mkdir -p "$(dirname "$DB")"

# Generate the bundled fixture on first use so the project is self-contained.
if [ ! -f "$HISTORY" ]; then
  SRCDIR="$ROOT/src" python3 -I - "$HISTORY" <<'PY'
import os, sys
from pathlib import Path
sys.path[:0] = [os.environ["SRCDIR"],
                "/workspace/pylibs/lib/python3.11/site-packages"]
from shot_catalog.fixturegen import generate_mixed_history
target = Path(sys.argv[1])
target.parent.mkdir(parents=True, exist_ok=True)
generate_mixed_history(target)
print(f"generated fixture {target}")
PY
fi

LOG_PORT="${LOG_PORT:-8081}"
QUERY_PORT="${QUERY_PORT:-8080}"

echo "starting log source on :$LOG_PORT serving $HISTORY"
python3 -I "$HERE/run_module.py" shot_catalog.log_store "$HISTORY" \
  --port "$LOG_PORT" --page-size 100 &
LOG_PID=$!

echo "starting query service on :$QUERY_PORT (db=$DB)"
python3 -I "$HERE/run_module.py" shot_catalog.server --db "$DB" \
  --log-source-url "http://127.0.0.1:$LOG_PORT" \
  --port "$QUERY_PORT" --page-size 100 --verbose &
SERVER_PID=$!

trap 'kill "$LOG_PID" "$SERVER_PID" 2>/dev/null || true' EXIT INT TERM
wait "$SERVER_PID"
