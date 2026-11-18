"""Crash-recovery: durable progress resumes from the same history prefix."""

import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

from shot_catalog.catalog import Catalog
from shot_catalog.fixturegen import generate_large_history
from shot_catalog.log_store import PagedLogSource, serve
from shot_catalog.sync import LogSourceClient, SyncEngine
import threading


def test_reopen_continues_from_durable_prefix(tmp_path):
    """A new Catalog over the same file sees the persisted snapshot prefix."""
    history = generate_large_history(tmp_path / "h.jsonl", n_shots=500)
    source = PagedLogSource(history["path"], page_size_hint=50,
                            shuffle=True, duplicate_rate=0.3, error_rate=0.1,
                            seed=3)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    db = tmp_path / "state.duckdb"
    try:
        with Catalog(db) as cat:
            eng = SyncEngine(cat, LogSourceClient(f"http://127.0.0.1:{port}"),
                             page_size=50, poll_interval=0.0)
            # Partial catch-up: stop after the first pass so some prefix is
            # durably committed but the whole history is not necessarily done.
            eng._pull_and_stage()
            eng._commit_ready_groups()
            prefix = cat.last_snapshot_seq()
            assert prefix > 0
            visible_partial = cat.stats()["visible_shots"]

        # Reopen the exact same file with a brand-new process/object.
        with Catalog(db) as cat2:
            assert cat2.last_snapshot_seq() == prefix
            assert cat2.rebuild_mismatch_count() == 0
            assert cat2.stats()["visible_shots"] == visible_partial
            # Continue catching up; redelivery is normal and harmless.
            eng2 = SyncEngine(cat2, LogSourceClient(f"http://127.0.0.1:{port}"),
                              page_size=50, poll_interval=0.0)
            eng2.run_until_caught_up(max_passes=40, idle_passes_to_stop=1)
            assert cat2.last_snapshot_seq() == history["total_events"]
            assert cat2.stats()["visible_shots"] == history["expected_visible_count"]
            assert cat2.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


def _run_worker(db_path, source_url, stop_after_seq, page_size):
    """Inline worker that syncs until the snapshot reaches stop_after_seq."""
    with Catalog(db_path) as cat:
        eng = SyncEngine(cat, LogSourceClient(source_url),
                         page_size=page_size, poll_interval=0.0)
        deadline = time.time() + 60
        while time.time() < deadline:
            eng._pull_and_stage()
            eng._commit_ready_groups()
            if cat.last_snapshot_seq() >= stop_after_seq:
                break
        return cat.last_snapshot_seq()


def test_progress_and_queries_always_describe_same_prefix(tmp_path):
    """The saved progress pointer can never run ahead of queryable state."""
    history = generate_large_history(tmp_path / "h2.jsonl", n_shots=800)
    source = PagedLogSource(history["path"], page_size_hint=100,
                            shuffle=True, duplicate_rate=0.4, error_rate=0.15,
                            seed=11)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    db = tmp_path / "state2.duckdb"
    try:
        # Sync in several independent "process restarts" over the same file.
        checkpoints = [300, 900, history["total_events"]]
        for target in checkpoints:
            _run_worker(db, f"http://127.0.0.1:{port}", target, 100)
            with Catalog(db) as reader:
                snap = reader.last_snapshot_seq()
                # The view's rows are exactly derivable from prefix [1..snap].
                assert reader.rebuild_mismatch_count() == 0
                # No staged-but-uncommitted seq below the snapshot may exist as
                # a queryable inconsistency: stats and snapshot agree.
                assert reader.stats()["snapshot_seq"] == snap
    finally:
        httpd.shutdown()


@pytest.mark.skipif(not os.path.exists("/workspace/shot_catalog_project/src"),
                    reason="project layout check")
def test_sigkill_then_restart_resumes(tmp_path):
    """Kill a real subprocess mid-sync; restart and verify full convergence."""
    from shot_catalog.fixturegen import generate_mixed_history

    history = generate_mixed_history(tmp_path / "mixed.jsonl")
    source = PagedLogSource(history["path"], page_size_hint=5,
                            shuffle=True, duplicate_rate=0.4, error_rate=0.2,
                            seed=55)
    httpd = serve(source, "127.0.0.1", 0)
    src_port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    db = tmp_path / "killed.duckdb"
    progress = tmp_path / "progress.txt"
    env = dict(os.environ)
    env["PYTHONPATH"] = "/workspace/shot_catalog_project/src:/workspace/pylibs/lib/python3.11/site-packages"
    worker = Path(__file__).resolve().parents[1] / "scripts" / "_sync_worker.py"
    # The mixed history has 24 events; freeze the worker after an intermediate
    # prefix so the SIGKILL provably lands while synchronization is in flight.
    env["STOP_AFTER_SEQ"] = "10"
    env["PROGRESS_FILE"] = str(progress)
    try:
        proc = subprocess.Popen(
            [sys.executable, "-I", str(worker), str(db),
             f"http://127.0.0.1:{src_port}", "5", "30"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        # Poll the sidecar progress file (the DuckDB file itself cannot be
        # opened by a second process while the worker holds the writer lock).
        deadline = time.time() + 30
        parked = False
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            if progress.exists():
                seq = int(progress.read_text().strip())
                if 0 < seq < history["total_events"]:
                    parked = True
                    break
            time.sleep(0.05)
        assert parked, "worker never reached an intermediate sync state"
        assert proc.poll() is None  # still alive and parked mid-sync
        proc.send_signal(signal.SIGKILL)  # hard kill, no cleanup
        proc.wait(timeout=10)
        assert proc.returncode != 0
        killed_prefix = int(progress.read_text().strip())
        assert 0 < killed_prefix < history["total_events"]

        # Restart WITHOUT the freeze: resume from durable state and converge.
        resume_env = {k: v for k, v in env.items()
                      if k not in ("STOP_AFTER_SEQ", "PROGRESS_FILE")}
        proc2 = subprocess.run(
            [sys.executable, "-I", str(worker), str(db),
             f"http://127.0.0.1:{src_port}", "5", "0"],
            env=resume_env, capture_output=True, text=True, timeout=60,
        )
        assert proc2.returncode == 0, proc2.stderr
        with Catalog(db) as cat:
            assert cat.last_snapshot_seq() == history["total_events"]
            assert cat.rebuild_mismatch_count() == 0
            assert cat.get_shot("film-b", "S012") is not None
            assert cat.get_shot("film-b", "S010") is None
    finally:
        httpd.shutdown()
