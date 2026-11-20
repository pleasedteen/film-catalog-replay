#!/usr/bin/env python3
"""One-shot hot/cold catch-up worker for kill-recovery tests.

Usage:
    _cold_worker.py <db_path> <log_source_url> <page_size> <max_seconds>

Environment:
    COLD_DIR          cold archive directory (required)
    ARCHIVE_ROWS      target rows per Parquet segment (default 8)
    KILL_COLD_OP      when set, the process SIGKILLs itself the first time the
                      cold storage performs this file operation (e.g.
                      "segment_rename"), simulating a hard kill in the middle
                      of an archival step
    STOP_AFTER_SEQ / PROGRESS_FILE  same parking sidecar as _sync_worker.py

Runs with -I and explicit sys.path entries (see run_tests.py bootstrap).
"""

import os
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(PROJECT_ROOT / "src"),
    "/workspace/pylibs/lib/python3.11/site-packages",
]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.cold import ColdStorage  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine, make_archiver  # noqa: E402


def _write_progress(seq: int) -> None:
    path = os.environ.get("PROGRESS_FILE")
    if not path:
        return
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(str(seq))
    os.replace(tmp, path)


def main() -> int:
    db_path, url, page_size, max_seconds = (
        sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4]))
    cold_dir = os.environ["COLD_DIR"]
    archive_rows = int(os.environ.get("ARCHIVE_ROWS", "8"))
    kill_op = os.environ.get("KILL_COLD_OP")
    stop_after_seq = int(os.environ.get("STOP_AFTER_SEQ", "0"))
    killed = {"done": False}

    def failure_hook(operation: str) -> None:
        if kill_op and operation == kill_op and not killed["done"]:
            killed["done"] = True
            _write_progress(catalog.last_snapshot_seq())
            # Hard kill with no cleanup: DuckDB WAL + cold state table must
            # make the next process resume coherently.
            os.kill(os.getpid(), signal.SIGKILL)

    cold = ColdStorage(cold_dir, failure_hook=failure_hook)
    with Catalog(db_path, cold=cold) as catalog:
        client = LogSourceClient(url)
        total_events = client.total()

        def on_commit(_group):
            seq = catalog.last_snapshot_seq()
            _write_progress(seq)
            if stop_after_seq and seq >= stop_after_seq and seq < total_events:
                import time as _t
                while True:
                    _t.sleep(3600)

        engine = SyncEngine(
            catalog, client, page_size=page_size, poll_interval=0.0,
            on_commit=on_commit,
            archiver=make_archiver(catalog, target_rows=archive_rows),
        )
        if max_seconds > 0 and not stop_after_seq:
            import time
            deadline = time.time() + max_seconds
            while time.time() < deadline:
                total = client.total()
                if catalog.last_snapshot_seq() >= total and total > 0:
                    break
                engine._pull_and_stage(start_offset=engine._resume_offset())
                engine._commit_ready_groups()
        else:
            engine.run_until_caught_up(max_passes=200, idle_passes_to_stop=1)
        _write_progress(catalog.last_snapshot_seq())
        print(f"snapshot_seq={catalog.last_snapshot_seq()} "
              f"cold_segments={catalog.stats()['cold_segments']} "
              f"oracle={catalog.verify_full_history_oracle()}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
