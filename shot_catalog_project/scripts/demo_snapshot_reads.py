#!/usr/bin/env python3
"""One-command demonstration of snapshot reads on the bundled fixtures.

It proves, with the shipped data only (fixtures/*.jsonl, generated on demand),
that reads are decoupled from writes:

  1. WRITE PAUSE -- a cross-page same-batch revision is parked *inside* its
     uncommitted transaction (all delta work done, COMMIT not yet issued).
     Point queries, the whole-film listing and the health check still answer
     immediately, and they answer with the previously published version.
     After the batch commits it becomes visible atomically.

  2. SLOW READ -- a client streams the 100k-shot film listing. While that
     response is still draining, later commit groups publish (the writer is
     never held by the reader). The finished listing still corresponds to
     exactly one published version, and the server emits it in bounded
     memory chunks.

  3. DISCONNECT -- a client abandons a listing mid-stream. The request's
     snapshot is released immediately, and synchronization continues to
     convergence afterwards.

Run:  python3 -I scripts/demo_snapshot_reads.py
"""

import http.client
import json
import resource
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"),
                "/workspace/pylibs/lib/python3.11/site-packages"]

from http.server import ThreadingHTTPServer  # noqa: E402

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.fixturegen import (  # noqa: E402
    describe_history,
    generate_large_history,
    generate_mixed_history,
)
from shot_catalog.log_format import normalize  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.server import build_handler  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402

FIXTURES = PROJECT_ROOT / "fixtures"


def rss_mb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024


def heading(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def ok(msg: str) -> None:
    print(f"  \033[1;32mPASS\033[0m {msg}")


def bundled_history(name: str, n_shots: int) -> tuple[Path, dict]:
    """Prefer the bundled fixture verbatim; only generate when it is absent."""
    path = FIXTURES / name
    if path.is_file():
        print(f"  using bundled fixture {name} (not regenerated)")
        return path, describe_history(path)
    history_dir = path.parent
    history_dir.mkdir(parents=True, exist_ok=True)
    print(f"  bundled fixture {name} missing; generating it into {history_dir}")
    if name == "mixed_history.jsonl":
        return path, generate_mixed_history(path)
    return path, generate_large_history(path, n_shots)


def start_query_service(catalog: Catalog):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(catalog))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def http_get(base: str, path: str, timeout: float = 10.0):
    t0 = time.time()
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return resp.status, payload, time.time() - t0
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8")), time.time() - t0


def stage_through(catalog: Catalog, client: LogSourceClient, last_seq: int,
                  page_size: int) -> None:
    """Durably stage source prefix [1, last_seq] (no commits)."""
    offset = 0
    while offset < last_seq:
        page = client.fetch_page(offset, page_size)
        catalog.stage_records([normalize(r) for r in page["records"]])
        offset += page_size


def scenario_write_pause(workdir: Path) -> None:
    heading("1. WRITE PAUSE -- queries answer the last published version")
    history_path, summary = bundled_history("mixed_history.jsonl", 0)
    batch = summary["batch"]

    source = PagedLogSource(history_path, page_size_hint=2, shuffle=False,
                            duplicate_rate=0.0, error_rate=0.0)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    catalog = Catalog(workdir / "pause.duckdb")
    q_httpd, base = start_query_service(catalog)
    try:
        client = LogSourceClient(f"http://127.0.0.1:{log_httpd.server_address[1]}")
        stage_through(catalog, client, batch["start_seq"] - 1, 2)
        SyncEngine(catalog, client, page_size=2)._commit_ready_groups()
        published = catalog.last_snapshot_seq()
        print(f"  published prefix before the batch: seq {published}")

        # Stage the batch and everything after it, then publish with the
        # batch group frozen just before COMMIT.
        stage_through(catalog, client, client.total(), 2)
        parked, release = threading.Event(), threading.Event()

        def before_commit(group):
            if group.batch_id == batch["id"]:
                parked.set()
                release.wait(60)

        engine = SyncEngine(catalog, client, page_size=2,
                            before_commit=before_commit)
        sync_thread = threading.Thread(target=engine._commit_ready_groups)
        sync_thread.start()
        try:
            assert parked.wait(30), "batch never reached the pre-commit seam"
            print(f"  batch {batch['id']} is parked before COMMIT "
                  f"(writer transaction open, snapshot still {published})")
            code, health, dt_h = http_get(base, "/health")
            code2, film, dt_f = http_get(base, "/film/film-a")
            code3, shot, dt_s = http_get(base, "/shot?film_id=film-a&shot_id=S001")
            print(f"  /health {code} in {dt_h*1000:.1f}ms | /film/film-a {code2} in "
                  f"{dt_f*1000:.1f}ms | /shot {code3} in {dt_s*1000:.1f}ms")
            print(f"  S001 while paused: duration_ms={shot['duration_ms']} "
                  f"(batch value is 13100), film snapshot={film['snapshot_seq']}")
            assert (code, code2, code3) == (200, 200, 200)
            assert max(dt_h, dt_f, dt_s) < 2.0
            assert health["snapshot_seq"] == film["snapshot_seq"] == published
            assert shot["duration_ms"] == 13000 and film["count"] == 3
            assert sync_thread.is_alive(), "the writer must still be mid-transaction"
            ok("point query, film listing and health check answered from the "
               "previous version while the batch was uncommitted")
        finally:
            release.set()
            sync_thread.join(timeout=60)

        code, shot, _ = http_get(base, "/shot?film_id=film-a&shot_id=S001")
        print(f"  after COMMIT: S001 duration_ms={shot['duration_ms']} "
              f"repair_note={shot['repair_note']!r}")
        assert shot["duration_ms"] == 13100
        assert shot["repair_note"] == "批量校准 13100ms"
        assert catalog.rebuild_mismatch_count() == 0
        ok("batch published atomically afterwards; oracle mismatch = 0")
    finally:
        q_httpd.shutdown()
        catalog.close()
        log_httpd.shutdown()


def scenario_slow_read(workdir: Path) -> None:
    heading("2. SLOW READ -- 100k listing drains while later batches publish")
    history_path, summary = bundled_history("large_history.jsonl", 100_000)
    batch = summary["batch"]
    page = 5_000

    source = PagedLogSource(history_path, page_size_hint=page, shuffle=False,
                            duplicate_rate=0.0, error_rate=0.0)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    catalog = Catalog(workdir / "slow.duckdb")
    q_httpd, _base = start_query_service(catalog)
    conn = None
    try:
        client = LogSourceClient(f"http://127.0.0.1:{log_httpd.server_address[1]}")
        stage_through(catalog, client, batch["start_seq"] - 1, page)
        SyncEngine(catalog, client, page_size=page)._commit_ready_groups()
        published = catalog.last_snapshot_seq()
        # Everything after the prefix is already staged, so the writer only
        # has to commit: it can publish while the client is still reading.
        stage_through(catalog, client, client.total(), page)
        print(f"  published prefix before the batch: seq {published}; "
              f"film-bulk has {catalog.stats()['visible_shots']} shots")

        conn = http.client.HTTPConnection("127.0.0.1", q_httpd.server_address[1],
                                          timeout=120)
        conn.request("GET", "/film/film-bulk")
        resp = conn.getresponse()
        assert resp.status == 200 and resp.chunked
        head = resp.read(8_192)
        print(f"  response is chunked; client has read only {len(head)} bytes "
              f"and is now idle (a slow client)")

        # The client is NOT draining. Publish the 100k-record batch + tail in
        # full; a reader holding the request must not block any of it.
        engine = SyncEngine(catalog, client, page_size=page)
        t0 = time.time()
        engine.run_until_caught_up(max_passes=5, idle_passes_to_stop=1)
        publish_seconds = time.time() - t0
        after = catalog.last_snapshot_seq()
        print(f"  writer published up to seq {after} in {publish_seconds:.1f}s "
              f"while the client had not drained the listing")
        assert after == summary["total_events"]
        assert catalog.active_readers == 1

        # Now let the slow client finish; the answer is still one version.
        body = head + resp.read()
        conn.close()
        conn = None
        listing = json.loads(body)
        print(f"  listing: snapshot_seq={listing['snapshot_seq']} "
              f"count={listing['count']} bytes={len(body):,}")
        assert listing["snapshot_seq"] == published
        assert listing["count"] == len(listing["shots"]) == summary["n_shots"]
        pre_batch = {(10 + (i % 7)) * 1000 for i in range(1, summary["n_shots"] + 1)}
        assert {s["duration_ms"] for s in listing["shots"]} == pre_batch
        current = catalog.get_shot("film-bulk", summary["sample_shot"])
        assert current["duration_ms"] == summary["sample_duration_ms"]
        ok("one response = one published version, even though the batch "
           "published while the client was still reading")
        ok(f"later batches kept publishing during the slow read "
           f"({publish_seconds:.1f}s, snapshot {published} -> {after})")
    finally:
        if conn is not None:
            conn.close()
        q_httpd.shutdown()
        catalog.close()
        log_httpd.shutdown()


def scenario_disconnect(workdir: Path) -> None:
    heading("3. DISCONNECT -- abandoned request releases its snapshot, sync continues")
    history_path, summary = bundled_history("large_history.jsonl", 100_000)
    batch = summary["batch"]
    page = 5_000

    source = PagedLogSource(history_path, page_size_hint=page, shuffle=False,
                            duplicate_rate=0.0, error_rate=0.0)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    catalog = Catalog(workdir / "drop.duckdb")
    q_httpd, base = start_query_service(catalog)
    try:
        client = LogSourceClient(f"http://127.0.0.1:{log_httpd.server_address[1]}")
        stage_through(catalog, client, batch["start_seq"] - 1, page)
        SyncEngine(catalog, client, page_size=page)._commit_ready_groups()
        print(f"  published prefix: seq {catalog.last_snapshot_seq()}")

        conn = http.client.HTTPConnection("127.0.0.1", q_httpd.server_address[1],
                                          timeout=30)
        conn.request("GET", "/film/film-bulk")
        resp = conn.getresponse()
        resp.read(1_024)  # read a little, then walk away
        assert catalog.active_readers == 1
        print(f"  client abandons the response with {catalog.active_readers} "
              f"pinned snapshot")
        conn.sock.close()
        conn.close()

        deadline = time.time() + 10
        while catalog.active_readers and time.time() < deadline:
            time.sleep(0.02)
        assert catalog.active_readers == 0, "abandoned request leaked its snapshot"
        ok("snapshot released immediately after the disconnect")

        engine = SyncEngine(catalog, client, page_size=page)
        engine.run_until_caught_up(max_passes=5, idle_passes_to_stop=1)
        assert catalog.last_snapshot_seq() == summary["total_events"]
        assert catalog.rebuild_mismatch_count() == 0
        assert catalog.stats()["visible_shots"] == summary["expected_visible_count"]
        code, health, _ = http_get(base, "/health")
        assert code == 200 and health["snapshot_seq"] == summary["total_events"]
        ok("synchronization continued to convergence after the disconnect "
           f"(snapshot {health['snapshot_seq']}, {health['visible_shots']} visible shots)")
    finally:
        q_httpd.shutdown()
        catalog.close()
        log_httpd.shutdown()


def main() -> int:
    import tempfile

    workdir = Path(tempfile.mkdtemp(prefix="shotcat-reads-"))
    print(f"work dir: {workdir}")
    scenario_write_pause(workdir)
    scenario_slow_read(workdir)
    scenario_disconnect(workdir)
    heading("SNAPSHOT READ DEMO OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
