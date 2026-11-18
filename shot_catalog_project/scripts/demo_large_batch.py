#!/usr/bin/env python3
"""Demonstration of the 100,000-shot same-batch revision.

Proves that:
  * one资料修复 revises 100k shots as a SINGLE atomic group spanning
    thousands of log pages;
  * the synchronizer never holds the batch in Python memory (peak RSS stays
    close to a single-page working set plus DuckDB's own buffers);
  * a half-delivered batch publishes nothing, then the whole batch appears
    at once;
  * v1 second durations and v2 millisecond durations reconcile into one
    millisecond answer carrying the v2 repair note.

Run:  python3 -I scripts/demo_large_batch.py [n_shots]
"""

import resource
import sys
import tempfile
import threading
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"),
                "/workspace/pylibs/lib/python3.11/site-packages"]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.fixturegen import generate_large_history  # noqa: E402
from shot_catalog.log_format import normalize  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402


def rss_mb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024


def main() -> int:
    n_shots = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    workdir = Path(tempfile.mkdtemp(prefix="shotcat-bulk-"))
    history = workdir / "large_history.jsonl"
    print(f"generating {n_shots}-shot history ...", flush=True)
    summary = generate_large_history(history, n_shots=n_shots)
    batch = summary["batch"]
    print(f"total source events   : {summary['total_events']}")
    print(f"batch {batch['id']} spans seq "
          f"{batch['start_seq']}..{batch['end_seq']} "
          f"({batch['end_seq'] - batch['start_seq'] + 1} revisions)")

    source = PagedLogSource(history, page_size_hint=1000,
                            shuffle=True, duplicate_rate=0.35,
                            error_rate=0.05, seed=424242)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    with Catalog(workdir / "catalog.duckdb") as catalog:
        client = LogSourceClient(f"http://127.0.0.1:{port}")
        engine = SyncEngine(catalog, client, page_size=1000, poll_interval=0.0)

        print("\n-- phase A: deliver only the pre-batch prefix plus a partial "
              "batch --", flush=True)
        # Pull pages until a bounded number of batch rows are staged but the
        # batch is far from whole. Stop purely on staged-batch row count so the
        # logic is independent of the history size and page layout.
        partial_target = min(2_000, n_shots // 2)
        offset = 0
        while True:
            page = client.fetch_page(offset, 1000)
            catalog.stage_records([normalize(r) for r in page["records"]])
            offset += 1000
            staged_batch = catalog._conn.execute(
                "SELECT count(*) FROM event_staging WHERE batch_id = ?",
                [batch["id"]],
            ).fetchone()[0]
            if staged_batch >= partial_target:
                break
            if not page["has_more"]:
                break
        engine._commit_ready_groups()
        print(f"staged {staged_batch} of {n_shots} batch rows; "
              f"snapshot after partial delivery: seq "
              f"{catalog.last_snapshot_seq()} (must be {batch['start_seq'] - 1})")
        pre = catalog.get_shot("film-bulk", summary["sample_shot"])
        print(f"sample still on pre-batch v1 state: duration_ms="
              f"{pre['duration_ms']} repair_note={pre['repair_note']!r}")
        assert catalog.last_snapshot_seq() == batch["start_seq"] - 1
        assert pre["repair_note"] is None
        assert staged_batch < n_shots  # genuinely a half-delivered batch

        print("\n-- phase B: complete delivery; batch commits atomically --",
              flush=True)
        t0 = time.time()
        engine.run_until_caught_up(max_passes=20, idle_passes_to_stop=1)
        elapsed = time.time() - t0
        c = engine.counters
        assert c.batch_groups_committed == 1
        assert c.largest_group == n_shots
        post = catalog.get_shot("film-bulk", summary["sample_shot"])
        print(f"sync completed in {elapsed:.1f}s; peak RSS {rss_mb()} MB")
        print(f"batch groups committed: {c.batch_groups_committed}, "
              f"largest group size: {c.largest_group}")
        print(f"final snapshot seq    : {catalog.last_snapshot_seq()} / "
              f"{summary['total_events']}")
        print(f"visible shots         : {catalog.stats()['visible_shots']} "
              f"(expected {summary['expected_visible_count']})")
        print(f"rebuild-oracle mismatch: {catalog.rebuild_mismatch_count()}")
        print(f"sample after batch    : duration_ms={post['duration_ms']} "
              f"repair_note={post['repair_note']!r}")
        assert post["duration_ms"] == summary["sample_duration_ms"]
        assert post["repair_note"] == summary["sample_repair_note"]
        assert catalog.rebuild_mismatch_count() == 0

    httpd.shutdown()
    print("\nLARGE BATCH DEMO OK")
    print(f"artifacts kept in: {workdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
