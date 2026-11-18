#!/usr/bin/env python3
"""Isolated-mode pytest entrypoint.

`python -I` ignores PYTHONPATH and user site-packages, so this bootstrap adds
the project's src tree and the dependency prefix explicitly before invoking
pytest. All test code therefore runs without ambient site-packages pollution.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path[:0] = [
    str(ROOT / "src"),
    "/workspace/pylibs/lib/python3.11/site-packages",
]

import pytest  # noqa: E402

if __name__ == "__main__":
    # Explicit node ids (files/::tests) are honored as given; with no arguments
    # the whole tests/ directory is collected.
    args = sys.argv[1:] or [str(ROOT / "tests")]
    sys.exit(pytest.main(args))
