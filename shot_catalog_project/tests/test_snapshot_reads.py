"""Snapshot reads: queries are decoupled from in-flight write transactions.

These tests pin down the properties the new read path exists for:

* a cross-page revision parked *before* COMMIT does not block a point query,
  a whole-film listing or the health check -- they answer from the last
  published version;
* a slow client streaming a large film listing keeps its own consistent
  snapshot while later batches publish behind it;
* a listing is produced from one published version even when commits land
  between its pages;
* a client that disconnects mid-stream releases its snapshot, and the
  synchronizer continues to converge.
"""

from __future__ import annotations

import http.client
import json
import threading
import time
import urllib.error
import urllib.request

import pytest

from shot_catalog.catalog import Catalog
from shot_catalog.fixturegen import generate_large_history, generate_mixed_history
from shot_catalog.log_format import normalize
from shot_catalog.log_store import PagedLogSource, serve
from shot_catalog.server import build_handler
from shot_catalog.sync import LogSourceClient, SyncEngine
from http.server import ThreadingHTTPServer


# --------------------------------------------------------------------- helpers
def _start_query(catalog):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(catalog))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _get(base, path, timeout=10):
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _sync_through(catalog, client, upto_seq, page_size):
    """Stage and commit exactly the source prefix [1, upto_seq]."""
    from shot_catalog.sync import SyncEngine

    engine = SyncEngine(catalog, client, page_size=page_size, poll_interval=0.0)
    offset = 0
    while offset < upto_seq:
        page = client.fetch_page(offset, page_size)
        catalog.stage_records([normalize(r) for r in page["records"]])
        offset += page_size
    engine._commit_ready_groups()
    assert catalog.last_snapshot_seq() == upto_seq
    return engine


@pytest.fixture()
def large_source(tmp_path):
    """20k-shot history: 20k v1 creates, one 20k batch, 4 lifecycle events."""
    history = generate_large_history(tmp_path / "big.jsonl", n_shots=20_000)
    source = PagedLogSource(history["path"], page_size_hint=2_000,
                            shuffle=False, duplicate_rate=0.0, error_rate=0.0)
    httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        yield history, f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()


# ------------------------------------------------------- paused write, live reads
def test_reads_answer_last_published_version_while_batch_is_paused(catalog, tmp_path):
    history = generate_mixed_history(tmp_path / "mixed.jsonl")
    source = PagedLogSource(history["path"], page_size_hint=2,
                            shuffle=False, duplicate_rate=0.0, error_rate=0.0)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    q_httpd, base = _start_query(catalog)
    try:
        client = LogSourceClient(f"http://127.0.0.1:{log_httpd.server_address[1]}")
        batch = history["batch"]
        _sync_through(catalog, client, batch["start_seq"] - 1, page_size=2)

        parked = threading.Event()
        release = threading.Event()

        def before_commit(group):
            if group.batch_id == batch["id"]:
                parked.set()
                assert release.wait(20), "test never released the parked batch"

        engine = SyncEngine(catalog, client, page_size=2, poll_interval=0.0,
                            before_commit=before_commit)
        # Stage the rest of the log (batch + tail) then try to publish it.
        offset = ((batch["start_seq"] - 1) // 2) * 2
        client_total = client.total()
        while offset < client_total:
            page = client.fetch_page(offset, 2)
            catalog.stage_records([normalize(r) for r in page["records"]])
            offset += 2
        sync_thread = threading.Thread(target=engine._commit_ready_groups)
        sync_thread.start()
        try:
            assert parked.wait(20), "batch group never reached the pre-commit seam"

            # The writer transaction is open and uncommitted. Every endpoint
            # must still answer, immediately, from snapshot start_seq-1.
            t0 = time.time()
            code, health = _get(base, "/health")
            code_film, film = _get(base, "/film/film-a")
            code_shot, shot = _get(base, "/shot?film_id=film-a&shot_id=S001")
            elapsed = time.time() - t0

            assert (code, code_film, code_shot) == (200, 200, 200)
            assert elapsed < 2.0, f"reads blocked on the parked batch ({elapsed:.2f}s)"
            assert health["snapshot_seq"] == batch["start_seq"] - 1
            assert film["snapshot_seq"] == batch["start_seq"] - 1
            assert film["count"] == len(film["shots"]) == 3
            # Pre-batch value: 13 seconds from the v1 revision, not the
            # batch's 13100 ms repair.
            assert shot["duration_ms"] == 13000
            # The earlier v2 note is still the published one; the batch's
            # "批量校准 13100ms" must not be visible yet.
            assert shot["repair_note"] == "修复时间码漂移"
            assert sync_thread.is_alive()  # still parked, i.e. mid-write
        finally:
            release.set()
            sync_thread.join(timeout=20)

        # After release the batch publishes atomically.
        assert catalog.last_snapshot_seq() == history["total_events"]
        code, shot = _get(base, "/shot?film_id=film-a&shot_id=S001")
        assert shot["duration_ms"] == 13100
        assert shot["repair_note"] == "批量校准 13100ms"
        assert catalog.rebuild_mismatch_count() == 0
    finally:
        q_httpd.shutdown()
        log_httpd.shutdown()


# --------------------------------------------- slow client vs. later publishing
def test_slow_listing_keeps_its_version_while_later_batches_publish(catalog, large_source):
    history, url = large_source
    client = LogSourceClient(url)
    batch = history["batch"]
    _sync_through(catalog, client, batch["start_seq"] - 1, page_size=2_000)

    q_httpd, base = _start_query(catalog)
    conn = http.client.HTTPConnection("127.0.0.1", q_httpd.server_address[1], timeout=60)
    try:
        conn.request("GET", "/film/film-bulk")
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.chunked
        # Read only a little, then leave the response in flight.
        head = resp.read(4_096)
        assert b'"snapshot_seq":' in head
        assert catalog.active_readers == 1

        # Now finish the whole remaining history, including the 20k-record
        # same-batch revision, while the slow response has not been drained.
        engine = SyncEngine(catalog, client, page_size=2_000, poll_interval=0.0)
        engine.run_until_caught_up(max_passes=20, idle_passes_to_stop=1)
        assert catalog.last_snapshot_seq() == history["total_events"]
        # Publishing really happened while the client was mid-stream.
        assert catalog.active_readers == 1

        # Drain the rest and reassemble the streamed body.
        body = head + resp.read()
        listing = json.loads(body)
        conn.close()
        # The handler releases the snapshot when the response completes; the
        # close is asynchronous to this client, so allow it a moment.
        deadline = time.time() + 10
        while catalog.active_readers and time.time() < deadline:
            time.sleep(0.02)
        assert listing["snapshot_seq"] == batch["start_seq"] - 1
        assert listing["count"] == len(listing["shots"]) == history["n_shots"]
        # Every row is the pre-batch version, even though the batch is
        # published now: one response <-> one published version.
        assert {s["duration_ms"] for s in listing["shots"]} == {
            (10 + (i % 7)) * 1000 for i in range(1, history["n_shots"] + 1)
        }
        current = catalog.get_shot("film-bulk", history["sample_shot"])
        assert current["duration_ms"] == history["sample_duration_ms"]

        code, stats = _get(base, "/stats")
        assert code == 200 and stats["snapshot_seq"] == history["total_events"]
        assert stats["visible_shots"] == history["expected_visible_count"]
        # Handler teardown trails the client's read; wait for the release.
        deadline = time.time() + 10
        while catalog.active_readers and time.time() < deadline:
            time.sleep(0.02)
        assert catalog.active_readers == 0
        assert catalog.rebuild_mismatch_count() == 0
    finally:
        conn.close()
        q_httpd.shutdown()


# ------------------------------------------------------------ partial iteration
def test_listing_pages_share_one_snapshot_across_a_commit(catalog, tmp_path):
    """A listing already in flight must not mix versions when a commit lands."""
    history = generate_large_history(tmp_path / "mid.jsonl", n_shots=400)
    source = PagedLogSource(history["path"], page_size_hint=400,
                            shuffle=False, duplicate_rate=0.0, error_rate=0.0)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    try:
        client = LogSourceClient(f"http://127.0.0.1:{log_httpd.server_address[1]}")
        batch = history["batch"]
        _sync_through(catalog, client, batch["start_seq"] - 1, page_size=400)

        with catalog.read_snapshot() as snap:
            gen = snap.iter_film_shots("film-bulk", page_size=37)
            first = [next(gen) for _ in range(37)]  # first page only
            assert all(s["duration_ms"] % 1000 != 250 for s in first)

            # Publish the batch revision mid-iteration, through the writer
            # connection, while this snapshot is pinned.
            offset = ((batch["start_seq"] - 1) // 400) * 400
            for _ in range(4):
                page = client.fetch_page(offset, 400)
                catalog.stage_records([normalize(r) for r in page["records"]])
                offset += 400
            engine = SyncEngine(catalog, client, page_size=400, poll_interval=0.0)
            engine._commit_ready_groups()
            assert catalog.last_snapshot_seq() == history["total_events"]

            rest = list(gen)
        rows = first + rest
        assert len(rows) == history["n_shots"]
        # Not one row leaked the newly published millisecond values.
        assert all(s["duration_ms"] % 1000 != 250 for s in rows)
    finally:
        log_httpd.shutdown()


# --------------------------------------------------------------- disconnect
def test_client_disconnect_releases_snapshot_and_sync_continues(catalog, large_source):
    history, url = large_source
    client = LogSourceClient(url)
    batch = history["batch"]
    _sync_through(catalog, client, batch["start_seq"] - 1, page_size=2_000)

    q_httpd, base = _start_query(catalog)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", q_httpd.server_address[1], timeout=10)
        conn.request("GET", "/film/film-bulk")
        resp = conn.getresponse()
        resp.read(1_024)
        assert catalog.active_readers == 1
        conn.sock.close()  # abrupt disconnect, no clean shutdown
        conn.close()

        deadline = time.time() + 10
        while catalog.active_readers and time.time() < deadline:
            time.sleep(0.02)
        assert catalog.active_readers == 0, "disconnected request leaked its snapshot"

        # Synchronization still converges after the disconnect.
        engine = SyncEngine(catalog, client, page_size=2_000, poll_interval=0.0)
        engine.run_until_caught_up(max_passes=20, idle_passes_to_stop=1)
        assert catalog.last_snapshot_seq() == history["total_events"]
        assert catalog.rebuild_mismatch_count() == 0
        assert catalog.stats()["visible_shots"] == history["expected_visible_count"]
        code, health = _get(base, "/health")
        assert code == 200 and health["snapshot_seq"] == history["total_events"]
    finally:
        q_httpd.shutdown()
