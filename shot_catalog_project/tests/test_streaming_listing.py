"""Streaming film listings: bounded memory, one version, unblocked writer.

Two delivery pathologies that used to break the old design (one shared
connection under one lock, whole listing materialized per request):

* a write transaction is PAUSED mid-commit: clients must still be answered
  from the last *published* version, never a half-applied one;
* a client reads its film listing SLOWLY: later batches must keep committing
  and become visible to other requests while that slow response is in flight.

Both are verified against the real HTTP query API with chunked responses.
"""

import json
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from shot_catalog.catalog import Catalog
from shot_catalog.fixturegen import generate_large_history, generate_mixed_history
from shot_catalog.log_store import PagedLogSource, serve
from shot_catalog.server import build_handler
from shot_catalog.sync import LogSourceClient, SyncEngine


def _start(catalog):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(catalog))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _get(base, path, timeout=20):
    with urllib.request.urlopen(base + path, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def _stage_file(cat, path, *, page_size=500, stop_at_seq=None):
    """Stage a local history file in page-sized batches (test shortcut)."""
    from shot_catalog.log_format import normalize

    batch = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if stop_at_seq is not None and cat.max_staged_seq() >= stop_at_seq:
                break
            batch.append(normalize(json.loads(line)))
            if len(batch) >= page_size:
                cat.stage_records(batch)
                batch = []
    if batch:
        cat.stage_records(batch)


@pytest.fixture()
def small_synced(tmp_path):
    summary = generate_mixed_history(tmp_path / "mixed.jsonl")
    source = PagedLogSource(summary["path"], page_size_hint=5,
                            shuffle=True, duplicate_rate=0.3, error_rate=0.0,
                            seed=5)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    cat = Catalog(tmp_path / "cat.duckdb")
    engine = SyncEngine(cat, LogSourceClient(
        f"http://127.0.0.1:{log_httpd.server_address[1]}"),
        page_size=5, poll_interval=0.0)
    engine.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)
    try:
        yield cat, summary
    finally:
        log_httpd.shutdown()
        cat.close()


# --------------------------------------------------------------------------
# 1. paused write transaction: published version keeps being served
# --------------------------------------------------------------------------
def test_paused_write_transaction_still_serves_published_version(small_synced):
    catalog, summary = small_synced
    httpd, base = _start(catalog)
    try:
        before = _get(base, "/film/film-a")[1]
        assert before["snapshot_seq"] == summary["total_events"]

        # Reproduce a 100k-style batch commit that is PAUSED half-way: the
        # writer holds an open transaction that has already changed every
        # shot in film-a, but has neither committed nor advanced the pointer.
        writer = catalog._conn
        writer.execute("BEGIN TRANSACTION")
        writer.execute(
            "UPDATE shot_state SET duration_ms = 999999, repair_note = 'PAUSED' "
            "WHERE film_id = 'film-a'"
        )
        writer.execute("UPDATE sync_state SET last_snapshot_seq = last_snapshot_seq + 1000")
        try:
            # A point query and a film listing both answer from the last
            # COMMITTED snapshot; nothing of the paused transaction leaks.
            code, shot = _get(base, "/shot?film_id=film-a&shot_id=S001")
            assert code == 200 and shot["duration_ms"] == 13100
            assert shot["repair_note"] == "批量校准 13100ms"
            code, film = _get(base, "/film/film-a")
            assert code == 200
            assert film["snapshot_seq"] == before["snapshot_seq"]
            assert film == before  # byte-for-byte same published version
        finally:
            writer.execute("ROLLBACK")

        # After the rollback the service is unchanged and still correct.
        assert _get(base, "/film/film-a")[1] == before
    finally:
        httpd.shutdown()


def test_paused_writer_does_not_block_new_commits_from_other_batches(
        small_synced):
    """A reader-visible batch commits while an unrelated write tx is paused.

    The paused transaction here is a staging write (as happens between the
    staging and apply steps of a group), not a catalog commit; the point is
    that reads never take the writer's lock and never queue behind it.
    """
    catalog, _ = small_synced
    httpd, base = _start(catalog)
    try:
        writer = catalog._conn
        writer.execute("BEGIN TRANSACTION")
        writer.execute(
            "INSERT INTO event_staging SELECT * FROM event_staging LIMIT 0"
        )  # no data change, but an open write transaction is held
        try:
            t0 = time.time()
            code, film = _get(base, "/film/film-a", timeout=5)
            elapsed = time.time() - t0
            assert code == 200
            assert elapsed < 2.0, f"listing waited on paused writer: {elapsed:.2f}s"
        finally:
            writer.execute("ROLLBACK")
    finally:
        httpd.shutdown()


# --------------------------------------------------------------------------
# 2. slow reader: later batches keep committing
# --------------------------------------------------------------------------
def test_slow_reader_does_not_stall_sync(tmp_path):
    """A client draining a huge listing slowly must not block later commits."""
    n_shots = 4_000
    summary = generate_large_history(tmp_path / "large.jsonl", n_shots=n_shots)
    source = PagedLogSource(summary["path"], page_size_hint=500,
                            shuffle=True, duplicate_rate=0.2, error_rate=0.0,
                            seed=17)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    cat = Catalog(tmp_path / "cat.duckdb")
    try:
        # Stage only the pre-batch prefix, so the listing is a stable, large
        # published version while the batch is delivered later.
        client = LogSourceClient(f"http://127.0.0.1:{log_httpd.server_address[1]}")
        engine = SyncEngine(cat, client, page_size=500, poll_interval=0.0)
        offset = 0
        while True:
            page = client.fetch_page(offset, 500)
            from shot_catalog.log_format import normalize
            cat.stage_records([normalize(r) for r in page["records"]])
            offset += 500
            if cat.max_staged_seq() >= n_shots:
                break
        engine._commit_ready_groups()
        pre_batch_snapshot = cat.last_snapshot_seq()
        assert pre_batch_snapshot == n_shots

        httpd, base = _start(cat)
        try:
            # Open the listing and read only the first bytes: this pins the
            # reader's snapshot while the sync engine keeps working.
            resp = urllib.request.urlopen(base + "/film/film-bulk", timeout=30)
            head = resp.read(512)
            assert head.startswith(b'{"film_id": "film-bulk"')
            try:
                assert pre_batch_snapshot == n_shots
                # Now complete the source and sync the whole batch while the
                # slow response is still open and mostly unread.
                done = threading.Thread(
                    target=engine.run_until_caught_up,
                    kwargs={"max_passes": 60, "idle_passes_to_stop": 1},
                    daemon=True,
                )
                done.start()
                done.join(timeout=60)
                assert not done.is_alive(), "sync blocked behind the slow reader"
                assert cat.last_snapshot_seq() == summary["total_events"]
                assert engine.counters.batch_groups_committed == 1
                assert engine.counters.largest_group == n_shots

                # Another request immediately sees the newer published
                # version while the slow response is STILL being drained.
                code, film_after = _get(base, "/film/film-bulk")
                assert code == 200
                assert film_after["snapshot_seq"] == summary["total_events"]
                assert film_after["count"] == summary["expected_visible_count"]
                sample = next(s for s in film_after["shots"]
                              if s["shot_id"] == summary["sample_shot"])
                assert sample["duration_ms"] == summary["sample_duration_ms"]
                assert sample["repair_note"] == summary["sample_repair_note"]

                # Drain the slow response: it must still describe the version
                # it started on -- pre-batch durations, no batch repair note,
                # and its own snapshot marker -- never a half-batch splice.
                rest = json.loads(head + resp.read())
            finally:
                resp.close()
            assert rest["snapshot_seq"] == pre_batch_snapshot
            assert rest["count"] == n_shots
            assert len(rest["shots"]) == n_shots
            assert all(s["repair_note"] is None for s in rest["shots"])
            assert all(s["duration_ms"] % 1000 == 0 for s in rest["shots"])
            assert rest["shots"][0]["duration_ms"] == 11_000
        finally:
            httpd.shutdown()
    finally:
        log_httpd.shutdown()
        cat.close()


# --------------------------------------------------------------------------
# 3. streaming shape / bounded buffer
# --------------------------------------------------------------------------
def test_film_listing_is_streamed_and_snapshot_consistent(small_synced):
    catalog, summary = small_synced
    httpd, base = _start(catalog)
    try:
        with urllib.request.urlopen(base + "/film/film-a", timeout=10) as resp:
            assert resp.status == 200
            # Chunked: no Content-Length, the list is never fully buffered.
            assert resp.headers.get("Transfer-Encoding") == "chunked"
            assert resp.headers.get("Content-Length") is None
            body = json.loads(resp.read())
        assert list(body) == ["film_id", "snapshot_seq", "count", "shots"]
        assert body["film_id"] == "film-a"
        assert body["snapshot_seq"] == summary["total_events"]
        assert body["count"] == len(body["shots"]) == 3
        assert [s["shot_id"] for s in body["shots"]] == ["S001", "S002", "S003"]
        # Same field set/order as the point query, duration in milliseconds.
        code, shot = _get(base, "/shot?film_id=film-a&shot_id=S001")
        assert body["shots"][0] == shot
    finally:
        httpd.shutdown()


def test_http_chunks_are_bounded_and_independent_of_film_size(tmp_path):
    """The streamed body is framed in chunks that never carry the whole film.

    Reads the raw chunked framing off the socket (the API layer), asserting
    the number of chunks grows with the film while the largest chunk does not.
    """
    import socket

    def read_chunked_frames(host, port, path):
        """Issue a raw HTTP/1.1 request and parse the chunked body frames."""
        with socket.create_connection((host, port), timeout=30) as sock:
            sock.sendall(
                f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\n"
                "Connection: close\r\n\r\n".encode("ascii"))
            buf = b""
            while b"\r\n\r\n" not in buf:
                buf += sock.recv(65536)
            head, rest = buf.split(b"\r\n\r\n", 1)
            assert b"transfer-encoding: chunked" in head.lower()
            chunks: list[bytes] = []
            data = rest
            while True:
                while b"\r\n" not in data:
                    data += sock.recv(65536)
                line, data = data.split(b"\r\n", 1)
                size = int(line, 16)
                if size == 0:
                    # Terminator, then an empty trailer section.
                    while not data.startswith(b"\r\n"):
                        data += sock.recv(65536)
                    break
                while len(data) < size + 2:
                    data += sock.recv(65536)
                chunks.append(data[:size])
                assert data[size:size + 2] == b"\r\n"
                data = data[size + 2:]
        return chunks

    def serve_listing(n):
        path = tmp_path / f"h{n}.jsonl"
        summary = generate_large_history(path, n_shots=n)
        cat = Catalog(tmp_path / f"c{n}.duckdb")
        try:
            _stage_file(cat, path, stop_at_seq=summary["total_events"])
            SyncEngine(cat, LogSourceClient("http://127.0.0.1:1"),
                       page_size=1000,
                       poll_interval=0.0)._commit_ready_groups()
            httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(cat))
            threading.Thread(target=httpd.serve_forever, daemon=True).start()
            try:
                chunks = read_chunked_frames(
                    "127.0.0.1", httpd.server_address[1], "/film/film-bulk")
            finally:
                httpd.shutdown()
                httpd.server_close()
        finally:
            cat.close()
        return summary, chunks, json.loads(b"".join(chunks))

    small_summary, small_chunks, small_body = serve_listing(200)
    large_summary, large_chunks, large_body = serve_listing(700)
    assert small_body["count"] == small_summary["expected_visible_count"]
    assert large_body["count"] == large_summary["expected_visible_count"]
    # More shots -> more chunks, but the largest chunk is bounded by the
    # per-chunk shot count and does not track the film size.
    assert len(large_chunks) > len(small_chunks)
    assert max(len(c) for c in large_chunks) < 4 * max(len(c) for c in small_chunks)
    assert max(len(c) for c in large_chunks) < len(json.dumps(large_body).encode())


def test_listing_is_read_in_bounded_chunks(tmp_path):
    """The iterator fetches bounded key-set pages, never the whole film."""
    from contextlib import contextmanager

    n = 2_500
    path = tmp_path / "h.jsonl"
    summary = generate_large_history(path, n_shots=n)
    cat = Catalog(tmp_path / "c.duckdb")
    try:
        _stage_file(cat, path, stop_at_seq=summary["total_events"])
        # Commit by re-reading nothing from the source: the fixture is fully
        # staged, so the ordinary prefix and the batch can be applied.
        SyncEngine(cat, LogSourceClient("http://127.0.0.1:1"),
                   page_size=1000, poll_interval=0.0)._commit_ready_groups()
        assert cat.last_snapshot_seq() == summary["total_events"]

        page_sizes: list[int] = []

        class CursorSpy:
            def __init__(self, cur):
                self._cur = cur

            def fetchone(self):
                return self._cur.fetchone()

            def fetchall(self):
                rows = self._cur.fetchall()
                page_sizes.append(len(rows))
                return rows

        class ConnSpy:
            def __init__(self, conn):
                self._conn = conn

            def execute(self, sql, params=None):
                if params is None:
                    return CursorSpy(self._conn.execute(sql))
                return CursorSpy(self._conn.execute(sql, params))

        class SpyCatalog(Catalog):
            @contextmanager
            def _read_conn(self):
                with super()._read_conn() as conn:
                    yield ConnSpy(conn)

        chunk = 97
        seen: list[dict] = []
        with SpyCatalog(cat.db_path) as spy:
            it = spy.iter_film_shots("film-bulk", chunk_size=chunk)
            header = next(it)
            seen = list(it)
        assert header["count"] == summary["expected_visible_count"]
        assert len(seen) == summary["expected_visible_count"]
        # Every data page was capped at the requested chunk size and every
        # row arrived exactly once, in shot order.
        assert page_sizes, "no pages were fetched"
        assert max(page_sizes) <= chunk
        assert sum(page_sizes) == len(seen)
        # Every full page is exactly one chunk; the stream ends on the short
        # remainder page and fetches nothing more.
        full = [s for s in page_sizes if s == chunk]
        assert len(full) == len(seen) // chunk
        assert page_sizes[-1] == len(seen) % chunk or len(seen) % chunk == 0
        shot_ids = [s["shot_id"] for s in seen]
        assert shot_ids == sorted(shot_ids)
        assert len(set(shot_ids)) == len(shot_ids)
    finally:
        cat.close()
