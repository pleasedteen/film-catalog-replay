"""Shared pytest fixtures: paths, sys.path bootstrap and in-process servers."""

import os
import sys
import threading

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
# The bootstrap (run_tests.py) normally puts these on sys.path already; keep
# this so tests also work under a plain pytest invocation in other layouts.
for path in (SRC, "/workspace/pylibs/lib/python3.11/site-packages"):
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402


@pytest.fixture()
def mixed_summary(tmp_path):
    from shot_catalog.fixturegen import generate_mixed_history

    path = tmp_path / "mixed.jsonl"
    return generate_mixed_history(path)


@pytest.fixture()
def log_server(mixed_summary):
    source = PagedLogSource(
        mixed_summary["path"], page_size_hint=5,
        error_rate=0.25, duplicate_rate=0.4, seed=12345,
    )
    httpd = serve(source, "127.0.0.1", 0)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


@pytest.fixture()
def catalog(tmp_path):
    with Catalog(tmp_path / "cat.duckdb") as cat:
        yield cat


@pytest.fixture()
def synced(catalog, log_server):
    engine = SyncEngine(
        catalog, LogSourceClient(log_server), page_size=5, poll_interval=0.0
    )
    engine.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)
    return catalog, engine
