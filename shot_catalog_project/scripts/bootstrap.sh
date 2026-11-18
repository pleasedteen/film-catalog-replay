#!/usr/bin/env bash
#
# Bootstrap dependencies into a self-contained prefix when no system pip or
# venv is available. Idempotent: re-running is cheap once everything exists.
#
# Override the prefix with:  PYLIBS=/some/path scripts/bootstrap.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PYLIBS="${PYLIBS:-/workspace/pylibs}"
SITE="$PYLIBS/lib/python3.11/site-packages"
PYTHON_BIN="$(command -v python3)"

echo "[bootstrap] python: $($PYTHON_BIN --version 2>&1)"
echo "[bootstrap] target prefix: $PYLIBS"

if PYTHONPATH="$SITE" "$PYTHON_BIN" -c "import duckdb, pytest" 2>/dev/null; then
  echo "[bootstrap] dependencies already present"
  exit 0
fi

# Prefer a normal pip if one exists; otherwise bootstrap pip into the prefix.
if "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
  "$PYTHON_BIN" -m pip install --prefix="$PYLIBS" -r "$ROOT/requirements.txt"
else
  WORK="$(mktemp -d)"
  trap 'rm -rf "$WORK"' EXIT
  echo "[bootstrap] no system pip; fetching get-pip.py (network required once)"
  curl -fsSL https://bootstrap.pypa.io/get-pip.py -o "$WORK/get-pip.py"
  "$PYTHON_BIN" "$WORK/get-pip.py" --prefix="$PYLIBS" --quiet
  PYTHONPATH="$SITE" "$PYTHON_BIN" -m pip install --prefix="$PYLIBS" \
    -r "$ROOT/requirements.txt"
fi

PYTHONPATH="$SITE" "$PYTHON_BIN" -c "import duckdb, pytest; \
print('[bootstrap] duckdb', duckdb.__version__, '/ pytest', pytest.__version__)"
