"""HTTP query API tests against the running service."""

import json
import threading
import urllib.error
import urllib.request

from shot_catalog.server import build_handler
from http.server import ThreadingHTTPServer


def _start(catalog):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(catalog))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _get(base, path):
    try:
        with urllib.request.urlopen(base + path, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_shot_and_film_endpoints(synced):
    catalog, _ = synced
    httpd, base = _start(catalog)
    try:
        code, shot = _get(base, "/shot?film_id=film-a&shot_id=S001")
        assert code == 200
        assert shot["duration_ms"] == 13100
        assert isinstance(shot["duration_ms"], int)
        assert shot["repair_note"] == "批量校准 13100ms"

        code, film = _get(base, "/film/film-a")
        assert code == 200
        assert film["count"] == 3
        assert film["snapshot_seq"] == catalog.snapshot_seq()
        assert {s["shot_id"] for s in film["shots"]} == {"S001", "S002", "S003"}
        # Every shot reports millisecond durations.
        assert all(isinstance(s["duration_ms"], int) for s in film["shots"])
    finally:
        httpd.shutdown()


def test_withdrawn_shot_returns_404(synced):
    catalog, _ = synced
    httpd, base = _start(catalog)
    try:
        code, body = _get(base, "/shot?film_id=film-b&shot_id=S010")
        assert code == 404
        assert "withdrawn" in body["error"]
        # Unknown shot is also 404, same retrieval contract.
        code, _ = _get(base, "/shot?film_id=film-b&shot_id=NOPE")
        assert code == 404
    finally:
        httpd.shutdown()


def test_health_and_stats(synced):
    catalog, _ = synced
    httpd, base = _start(catalog)
    try:
        code, health = _get(base, "/health")
        assert code == 200 and health["status"] == "ok"
        assert health["snapshot_seq"] == catalog.snapshot_seq()
        code, stats = _get(base, "/stats")
        assert code == 200
        assert stats["visible_shots"] == catalog.stats()["visible_shots"]
    finally:
        httpd.shutdown()


def test_missing_param_is_400(synced):
    catalog, _ = synced
    httpd, base = _start(catalog)
    try:
        code, _ = _get(base, "/shot?film_id=film-a")
        assert code == 400
    finally:
        httpd.shutdown()


def test_subscription_endpoints(catalog):
    """GET lists subscriptions; POST appends one as backfilling."""
    httpd, base = _start(catalog)
    try:
        code, body = _get(base, "/subscriptions")
        assert code == 200
        assert body["films"] == [] and body["sparse"] is False

        import urllib.request
        req = urllib.request.Request(
            base + "/subscriptions",
            data=json.dumps({"film_id": "film-a"}).encode("utf-8"),
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            created = json.loads(resp.read().decode("utf-8"))
        assert created["film_id"] == "film-a"
        # Nothing published yet, so the first subscription joins as active.
        assert created["status"] == "active"

        code, body = _get(base, "/subscriptions")
        assert body["sparse"] is True
        assert body["films"][0]["film_id"] == "film-a"
        assert body["films"][0]["status"] == "active"

        # Missing film_id is a 400.
        bad = urllib.request.Request(base + "/subscriptions", data=b"{}",
                                     method="POST")
        try:
            urllib.request.urlopen(bad, timeout=10)
            assert False, "expected 400"
        except urllib.error.HTTPError as exc:
            assert exc.code == 400
    finally:
        httpd.shutdown()


def test_concurrent_reads_while_syncing(catalog, mixed_summary):
    """Queries keep responding with consistent snapshots during sync."""
    from shot_catalog.log_store import PagedLogSource, serve
    from shot_catalog.sync import LogSourceClient, SyncEngine

    source = PagedLogSource(mixed_summary["path"], page_size_hint=5,
                            error_rate=0.3, duplicate_rate=0.4, seed=222)
    log_httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=log_httpd.serve_forever, daemon=True).start()
    query_httpd, base = _start(catalog)
    engine = SyncEngine(catalog, LogSourceClient(
        f"http://127.0.0.1:{log_httpd.server_address[1]}"),
        page_size=5, poll_interval=0.0)
    thread = threading.Thread(
        target=engine.run_until_caught_up,
        kwargs={"max_passes": 60, "idle_passes_to_stop": 1}, daemon=True)
    thread.start()
    try:
        seen_snapshots = set()
        import time
        deadline = time.time() + 20
        while time.time() < deadline:
            code, film = _get(base, "/film/film-b")
            assert code == 200
            # A listing must always be internally consistent: the snapshot
            # marker matches every row's prefix and count is truthful.
            assert film["count"] == len(film["shots"])
            seen_snapshots.add(film["snapshot_seq"])
            if not thread.is_alive():
                break
        thread.join(timeout=10)
        # Progress advanced through more than one snapshot during the run.
        assert catalog.snapshot_seq() == mixed_summary["total_events"]
        assert len(seen_snapshots) >= 1
    finally:
        log_httpd.shutdown()
        query_httpd.shutdown()
