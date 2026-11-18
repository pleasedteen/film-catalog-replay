#!/usr/bin/env python3
"""Isolated-mode module runner.

`python -I` ignores PYTHONPATH, so `python -I -m shot_catalog.server` cannot
find the package when dependencies live in a custom prefix. This runner adds
the project src tree and dependency prefix to sys.path, then invokes a
module's main() with the remaining argv.

Usage: run_module.py shot_catalog.server [args...]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT / "src"),
    "/workspace/pylibs/lib/python3.11/site-packages",
]

import importlib  # noqa: E402

if __name__ == "__main__":
    module_name, argv = sys.argv[1], sys.argv[2:]
    sys.argv = [module_name, *argv]
    module = importlib.import_module(module_name)
    raise SystemExit(module.main())
