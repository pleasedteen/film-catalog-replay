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


def test_reopen_backfills_hole_when_later_pages_arrived_first(tmp_path):
    """Restart resumes at the first missing page, not at the staged tail."""
    import json

    path = tmp_path / "tailish.jsonl"
    recs = [
        {"seq": 1, "event_id": "e1", "event_type": "upsert", "film_id": "f",
         "shot_id": "A", "version": 1, "ts": "2026-11-01T00:00:01Z",
         "payload": {"duration_sec": 1}},
        {"seq": 2, "event_id": "e2", "event_type": "upsert", "film_id": "f",
         "shot_id": "B", "version": 1, "ts": "2026-11-01T00:00:02Z",
         "payload": {"duration_sec": 2}},
    ]
    for i, seq in enumerate((3, 4, 5), start=1):
        recs.append({"seq": seq, "event_id": f"e{seq}", "event_type": "upsert",
                     "film_id": "f", "shot_id": f"T{i}", "version": 2,
                     "ts": f"2026-11-01T00:00:0{seq}Z", "batch_id": "B1",
                     "payload": {"duration_ms": 1000 + i,
                                 "repair_note": f"tail fix {i}"}})
    path.write_text("\n".join(json.dumps(r) for r in recs) + "\n",
                    encoding="utf-8")
    source = PagedLogSource(path, page_size_hint=2, shuffle=True,
                            duplicate_rate=0.4, error_rate=0.15, seed=9)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    db = tmp_path / "hole_reopen.duckdb"
    try:
        # First "process": only the later pages survive (as after a kill that
        # landed while the first page was still in flight).
        with Catalog(db) as cat:
            client = LogSourceClient(f"http://127.0.0.1:{port}")
            for offset in (4, 2):
                page = client.fetch_page(offset, 2)
                from shot_catalog.log_format import normalize
                cat.stage_records([normalize(r) for r in page["records"]])
            assert cat.max_staged_seq() == 5
            assert cat.last_snapshot_seq() == 0  # nothing publishable yet

        # Reopen with a brand-new engine: the missing first page must be
        # backfilled and the complete tail batch then published.
        with Catalog(db) as cat2:
            eng = SyncEngine(cat2, LogSourceClient(f"http://127.0.0.1:{port}"),
                             page_size=2, poll_interval=0.0)
            assert eng._resume_offset(total=5) == 0
            eng.run_until_caught_up(max_passes=30, idle_passes_to_stop=1)
            assert cat2.last_snapshot_seq() == 5
            assert eng.counters.batch_groups_committed == 1
            assert cat2.rebuild_mismatch_count() == 0
            assert cat2.get_shot("f", "T3")["repair_note"] == "tail fix 3"
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
