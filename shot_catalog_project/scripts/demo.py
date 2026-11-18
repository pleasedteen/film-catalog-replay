#!/usr/bin/env python3
"""End-to-end, repeatable demonstration of the shot catalog service.

It runs entirely on localhost with bundled fixtures and no external access:

  1. starts the local messy paged log source (shuffle + duplicates + errors);
  2. generates the mixed-version history if it is not present;
  3. synchronizes it into a fresh DuckDB catalog and prints the counters that
     prove duplicate/out-of-order delivery was handled;
  4. queries one shot and a whole film and prints millisecond durations and
     repair notes;
  5. demonstrates withdraw/republish visibility;
  6. demonstrates crash recovery by reopening the same DuckDB file and
     resuming from the persisted snapshot.

Run:  python3 -I scripts/demo.py
"""

import json
import os
import sys
import tempfile
import threading
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"),
                "/workspace/pylibs/lib/python3.11/site-packages"]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.fixturegen import generate_mixed_history  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402


def heading(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="shotcat-demo-"))
    history = workdir / "mixed_history.jsonl"
    db_path = workdir / "catalog.duckdb"
    summary = generate_mixed_history(history)

    source = PagedLogSource(history, page_size_hint=5,
                            shuffle=True, duplicate_rate=0.4,
                            error_rate=0.2, seed=20261118)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{port}"
    print(f"local log source on {url} serving {source.total} source records")
    print(f"durable DuckDB state: {db_path}")

    heading("1. Catch-up over shuffled / duplicated / error-prone delivery")
    with Catalog(db_path) as catalog:
        engine = SyncEngine(catalog, LogSourceClient(url),
                            page_size=5, poll_interval=0.0)
        engine.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
        c = engine.counters
        print(f"pages fetched            : {c.pages_fetched}")
        print(f"records seen (with dups) : {c.records_seen}")
        print(f"records newly staged     : {c.records_newly_staged}")
        print(f"commit groups            : {c.groups_committed}")
        print(f"same-batch groups        : {c.batch_groups_committed} "
              f"(largest = {c.largest_group} records, one atomic commit)")
        print(f"final snapshot seq       : {catalog.last_snapshot_seq()} "
              f"/ source total {summary['total_events']}")
        print(f"rebuild-oracle mismatch  : {catalog.rebuild_mismatch_count()}")

        heading("2. Query one shot (durations normalized to milliseconds)")
        s001 = catalog.get_shot("film-a", "S001")
        print(json.dumps(s001, ensure_ascii=False, indent=2))

        heading("3. Query the full shot list of film-a")
        film_a = catalog.list_film_shots("film-a")
        print(f"snapshot_seq={film_a['snapshot_seq']} count={film_a['count']}")
        for shot in film_a["shots"]:
            print(f"  {shot['shot_id']}: {shot['duration_ms']:>6} ms | "
                  f"repair_note={shot['repair_note']!r} | "
                  f"description={shot['description']!r}")

        heading("4. Withdraw / republish visibility")
        print("S010 ends withdrawn -> get_shot returns:",
              catalog.get_shot("film-b", "S010"))
        print("S012 was withdrawn then republished ->",
              catalog.get_shot("film-b", "S012")["title"])

    heading("5. Crash recovery: reopen the same file and resume")
    with Catalog(db_path) as reopened:
        prefix = reopened.last_snapshot_seq()
        print(f"reopened; persisted snapshot prefix = {prefix}")
        engine2 = SyncEngine(reopened, LogSourceClient(url),
                             page_size=5, poll_interval=0.0)
        before = reopened.stats()["staged_events"]
        engine2.run_until_caught_up(max_passes=20, idle_passes_to_stop=1)
        after = reopened.stats()
        print("redelivery staged zero new rows:",
              after["staged_events"] == before)
        print("rebuild-oracle mismatch after resume:",
              reopened.rebuild_mismatch_count())
        print("final visible shots:", after["visible_shots"])

    httpd.shutdown()
    heading("DEMO COMPLETE")
    print(f"artifacts kept in: {workdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
