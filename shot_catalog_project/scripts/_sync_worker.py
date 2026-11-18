#!/usr/bin/env python3
"""One-shot catch-up worker used by recovery tests and the demo.

Usage:
    _sync_worker.py <db_path> <log_source_url> <page_size> <max_seconds>

Syncs until the persisted snapshot covers the whole source history, or until
max_seconds elapses (0 = run to completion). Exit code 0 means fully caught up.
Runs with isolated mode (-I) so only explicit sys.path entries are used.
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(PROJECT_ROOT / "src"),
    "/workspace/pylibs/lib/python3.11/site-packages",
]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402


def _write_progress(seq: int) -> None:
    path = os.environ.get("PROGRESS_FILE")
    if not path:
        return
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(str(seq))
    os.replace(tmp, path)


def main() -> int:
    db_path, url, page_size, max_seconds = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
    # When set, the worker parks after the snapshot reaches this seq, so a
    # supervisor can reliably SIGKILL it while synchronization is in flight.
    stop_after_seq = int(os.environ.get("STOP_AFTER_SEQ", "0"))

    def on_commit(group):
        seq = catalog.last_snapshot_seq()
        _write_progress(seq)
        if stop_after_seq and seq >= stop_after_seq and seq < total_events:
            import time as _t
            while True:  # parked until killed; never fully converged
                _t.sleep(3600)

    with Catalog(db_path) as catalog:
        client = LogSourceClient(url)
        total_events = client.total()
        engine = SyncEngine(
            catalog, client, page_size=page_size, poll_interval=0.0,
            on_commit=on_commit,
        )
        if max_seconds > 0 and not stop_after_seq:
            import time
            deadline = time.time() + max_seconds
            while time.time() < deadline:
                try:
                    total = client.total()
                except Exception:
                    break
                if catalog.last_snapshot_seq() >= total and total > 0:
                    break
                engine._pull_and_stage(start_offset=engine._resume_offset())
                engine._commit_ready_groups()
        else:
            engine.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
        _write_progress(catalog.last_snapshot_seq())
        print(f"snapshot_seq={catalog.last_snapshot_seq()} "
              f"visible={catalog.stats()['visible_shots']}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
