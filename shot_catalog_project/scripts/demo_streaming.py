#!/usr/bin/env python3
"""Demonstrate the two streaming-response guarantees, entirely on local fixtures.

  A. PAUSED WRITE: while a write transaction is held open mid-commit (nothing
     published yet), clients still receive the last published version of a
     film's shot list -- never a partial batch.

  B. SLOW READER: a client that opens a 100k-scale shot list and drains it
     slowly does not stall synchronization. Later batches keep committing and
     other requests already see the newer snapshot, while the slow response
     stays pinned to the version it started on (same snapshot, count, shots).

Run:  python3 -I scripts/demo_streaming.py [n_shots]
"""

import json
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"),
                "/workspace/pylibs/lib/python3.11/site-packages"]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.fixturegen import (  # noqa: E402
    generate_large_history, generate_mixed_history,
)
from shot_catalog.log_format import normalize  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.server import SHOTS_PER_CHUNK, build_handler  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402


def heading(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def start_query_api(cat: Catalog):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(cat))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def get_json(base: str, path: str) -> dict:
    with urllib.request.urlopen(base + path, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def start_log_source(path, page_size):
    source = PagedLogSource(path, page_size_hint=page_size, shuffle=True,
                            duplicate_rate=0.3, error_rate=0.05, seed=2026)
    httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def scenario_paused_write(workdir: Path) -> bool:
    heading("A. write transaction paused mid-commit -> clients still see the published version")
    summary = generate_mixed_history(workdir / "mixed.jsonl")
    log_httpd, url = start_log_source(workdir / "mixed.jsonl", 5)
    cat = Catalog(workdir / "paused.duckdb")
    try:
        engine = SyncEngine(cat, LogSourceClient(url), page_size=5,
                            poll_interval=0.0)
        engine.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)
        httpd, base = start_query_api(cat)
        try:
            before = get_json(base, "/film/film-a")
            print(f"published snapshot_seq={before['snapshot_seq']} "
                  f"count={before['count']}")

            writer = cat._conn
            writer.execute("BEGIN TRANSACTION")
            writer.execute(
                "UPDATE shot_state SET duration_ms = 999999, "
                "repair_note = 'PAUSED-UNCOMMITTED' WHERE film_id = 'film-a'")
            writer.execute(
                "UPDATE sync_state SET last_snapshot_seq = last_snapshot_seq + 1000")
            try:
                during = get_json(base, "/film/film-a")
                shot = get_json(base, "/shot?film_id=film-a&shot_id=S001")
                print(f"...while paused: listing snapshot_seq={during['snapshot_seq']} "
                      f"count={during['count']} (uncommitted rows invisible)")
                print(f"...while paused: S001 duration_ms={shot['duration_ms']} "
                      f"repair_note={shot['repair_note']!r}")
                ok = (during == before
                      and shot["duration_ms"] == 13100
                      and "PAUSED" not in (shot["repair_note"] or ""))
            finally:
                writer.execute("ROLLBACK")
            after = get_json(base, "/film/film-a")
            ok = ok and after == before
            print("PAUSED-WRITE PASS" if ok else "PAUSED-WRITE FAIL")
            return ok
        finally:
            httpd.shutdown()
    finally:
        log_httpd.shutdown()
        cat.close()


def scenario_slow_reader(workdir: Path, n_shots: int) -> bool:
    heading(f"B. slow reader of a {n_shots}-shot listing -> later batches still commit")
    summary = generate_large_history(workdir / "large.jsonl", n_shots=n_shots)
    log_httpd, url = start_log_source(workdir / "large.jsonl", 500)
    cat = Catalog(workdir / "slow.duckdb")
    try:
        client = LogSourceClient(url)
        engine = SyncEngine(cat, client, page_size=500, poll_interval=0.0)
        # Publish only the pre-batch prefix, so the listing has content to
        # stream while the giant batch is still outstanding.
        offset = 0
        while cat.max_staged_seq() < n_shots:
            page = client.fetch_page(offset, 500)
            cat.stage_records([normalize(r) for r in page["records"]])
            offset += 500
        engine._commit_ready_groups()
        pre = cat.last_snapshot_seq()
        print(f"published pre-batch snapshot_seq={pre} ({n_shots} shots)")

        httpd, base = start_query_api(cat)
        try:
            # Open the listing; read only the tip of it, then stop draining.
            resp = urllib.request.urlopen(base + "/film/film-bulk", timeout=60)
            head = resp.read(1024)
            try:
                sync_thread = threading.Thread(
                    target=engine.run_until_caught_up,
                    kwargs={"max_passes": 60, "idle_passes_to_stop": 1},
                    daemon=True)
                t0 = time.time()
                sync_thread.start()
                sync_thread.join(timeout=60)
                elapsed = time.time() - t0
                stalled = sync_thread.is_alive()
                print(f"sync of batch B900 while the reader is slow: "
                      f"{'STALLED' if stalled else f'finished in {elapsed:.2f}s'}")
                print(f"batch groups committed={engine.counters.batch_groups_committed} "
                      f"largest group={engine.counters.largest_group}")
                newer = get_json(base, "/film/film-bulk")
                print(f"a separate request now sees snapshot_seq={newer['snapshot_seq']} "
                      f"count={newer['count']}")
                sample = next(s for s in newer["shots"]
                              if s["shot_id"] == summary["sample_shot"])
                print(f"newer request sample: duration_ms={sample['duration_ms']} "
                      f"repair_note={sample['repair_note']!r}")

                # Drain the slow response and confirm it is pinned to `pre`.
                rest = json.loads(head + resp.read())
                pinned = (rest["snapshot_seq"] == pre
                          and rest["count"] == n_shots
                          and len(rest["shots"]) == n_shots
                          and all(s["repair_note"] is None for s in rest["shots"])
                          and all(s["duration_ms"] % 1000 == 0
                                  for s in rest["shots"]))
                print(f"slow response: snapshot_seq={rest['snapshot_seq']} "
                      f"count={rest['count']} (still the pre-batch version: {pinned})")
            finally:
                resp.close()

            ok = (not stalled
                  and engine.counters.batch_groups_committed == 1
                  and engine.counters.largest_group == n_shots
                  and newer["snapshot_seq"] == summary["total_events"]
                  and newer["count"] == summary["expected_visible_count"]
                  and sample["duration_ms"] == summary["sample_duration_ms"]
                  and pinned)
            print(f"(listing is streamed in HTTP chunks of {SHOTS_PER_CHUNK} shots; "
                  f"Python holds one bounded page at a time)")
            print("SLOW-READER PASS" if ok else "SLOW-READER FAIL")
            return ok
        finally:
            httpd.shutdown()
    finally:
        log_httpd.shutdown()
        cat.close()


def main() -> int:
    n_shots = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    workdir = Path(tempfile.mkdtemp(prefix="shotcat-stream-"))
    ok_a = scenario_paused_write(workdir)
    ok_b = scenario_slow_reader(workdir, n_shots)
    heading("STREAMING DEMO COMPLETE")
    print(f"paused write: {'PASS' if ok_a else 'FAIL'}; "
          f"slow reader: {'PASS' if ok_b else 'FAIL'}")
    return 0 if (ok_a and ok_b) else 1


if __name__ == "__main__":
    raise SystemExit(main())
