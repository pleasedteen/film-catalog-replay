"""Sparse subscription semantics: only subscribed films cross the wire.

These tests pin down the behaviors the warehouse needs as its library grows:

* paging delivers only the subscribed films' shot events, so transferred
  volume does not grow with unsubscribed ordinary revisions;
* the global snapshot still advances across the whole library;
* a cross-film same-batch revision publishes its subscribed survivors as one
  atomic group, despite the unsubscribed remainder never being transferred;
* append subscriptions backfill only the new film, without disturbing the
  already-active ones, and become queryable at the same published version;
* an interrupted backfill resumes from its durable progress on restart.
"""

import threading
import time

import pytest

from shot_catalog.catalog import Catalog
from shot_catalog.fixturegen import generate_sparse_history
from shot_catalog.log_format import normalize
from shot_catalog.log_store import PagedLogSource, serve
from shot_catalog.sync import LogSourceClient, SyncEngine


def _serve(path, *, page_size=50, shuffle=False, dup=0.0, err=0.0, seed=1):
    source = PagedLogSource(path, page_size_hint=page_size, shuffle=shuffle,
                            duplicate_rate=dup, error_rate=err, seed=seed)
    httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return source, httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def test_log_store_film_filter_and_batch_runs(sparse_history):
    """The paging endpoint filters records by film but keeps global window info."""
    source = PagedLogSource(sparse_history["path"], page_size_hint=50,
                            shuffle=False, duplicate_rate=0.0, error_rate=0.0)
    page = source.serve_page(sparse_history["skip"]["start_seq"] - 1, 30,
                             films={"film-sub-a", "film-sub-b"})
    # Every returned record belongs to a subscribed film...
    assert all(r["film_id"] in ("film-sub-a", "film-sub-b")
               for r in page["records"])
    # ...but the window, total and batch-extent reporting stay global.
    assert page["total"] == sparse_history["total_events"]
    assert page["has_more"] is True

    batch = sparse_history["batch"]
    page_at_batch = source.serve_page(batch["start_seq"] - 1, 30,
                                      films={"film-sub-a"})
    runs = {r["batch_id"]: r for r in page_at_batch["batch_runs"]}
    assert batch["id"] in runs
    # The full extent is reported even though only one subscribed member's
    # record was transferred in this page.
    assert runs[batch["id"]]["start_seq"] == batch["start_seq"]
    assert runs[batch["id"]]["end_seq"] == batch["end_seq"]
    assert all(r["film_id"] == "film-sub-a" for r in page_at_batch["records"])


@pytest.fixture()
def sparse_history(tmp_path):
    return generate_sparse_history(tmp_path / "sparse.jsonl",
                                   n_unsubscribed=400, batch_across=10)


def _engine(catalog, url, page_size=50):
    return SyncEngine(catalog, LogSourceClient(url), page_size=page_size,
                      poll_interval=0.0)


def test_only_subscribed_films_are_transferred(sparse_history):
    """No unsubscribed record is fetched or staged, however much churn."""
    source, httpd, url = _serve(sparse_history["path"])
    try:
        with Catalog(sparse_history["path"] + ".duckdb") as cat:
            cat.add_subscription("film-sub-a")
            cat.add_subscription("film-sub-b")
            eng = _engine(cat, url)
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)

            # The scan window may cover the skip film's seqs, but its records
            # are filtered out by the source -- only subscribed shot events were
            # transferred and staged.
            transferred_films = {
                r[0] for r in cat._conn.execute(
                    "SELECT DISTINCT film_id FROM event_staging"
                ).fetchall()
            }
            assert transferred_films == {"film-sub-a", "film-sub-b"}
            assert cat.get_shot("film-skip", "SK001") is None
            assert cat.rebuild_mismatch_count() == 0
            # The whole 400-revision churn was skipped; transferred payload is
            # a small constant, not proportional to the library.
            assert eng.counters.records_transferred < sparse_history["total_events"] // 4
    finally:
        httpd.shutdown()


def test_transferred_volume_is_flat_across_library_size(tmp_path):
    """Doubling unsubscribed churn must not change how many records transfer."""
    def run(n_skip):
        hist = generate_sparse_history(tmp_path / f"h{n_skip}.jsonl",
                                       n_unsubscribed=n_skip, batch_across=8)
        source, httpd, url = _serve(hist["path"])
        try:
            with Catalog(tmp_path / f"c{n_skip}.duckdb") as cat:
                cat.add_subscription("film-sub-a")
                cat.add_subscription("film-sub-b")
                eng = _engine(cat, url)
                eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
                return eng.counters.records_transferred, hist["total_events"]
        finally:
            httpd.shutdown()

    small_xfer, small_total = run(200)
    big_xfer, big_total = run(400)
    assert big_total > small_total + 100  # the library really grew
    # Transferred payload is bounded by the subscribed films' events, not the
    # library: the same handful of subscribed records crosses either way.
    assert big_xfer == small_xfer


def test_global_snapshot_advances_past_unsubscribed_history(sparse_history):
    source, httpd, url = _serve(sparse_history["path"])
    try:
        with Catalog(sparse_history["path"] + ".duckdb") as cat:
            cat.add_subscription("film-sub-a")
            eng = _engine(cat, url)
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
            # Snapshot (and fetch watermark) reach the global tail even though
            # almost all of the history was never transferred.
            assert cat.last_snapshot_seq() == sparse_history["total_events"]
            assert cat.resolved_seq() == sparse_history["total_events"]
            assert cat.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


def test_cross_film_batch_publishes_subscribed_survivors_atomically(sparse_history):
    """A subscribed batch member appears at the batch commit with its values."""
    source, httpd, url = _serve(sparse_history["path"])
    try:
        with Catalog(sparse_history["path"] + ".duckdb") as cat:
            cat.add_subscription("film-sub-a")
            cat.add_subscription("film-sub-b")
            observed = []
            eng = _engine(cat, url)
            orig = cat.apply_publication

            def spy(start, end):
                orig(start, end)
                shot = cat.get_shot("film-sub-a", "A001")
                observed.append((start, end, shot["duration_ms"] if shot else None))

            cat.apply_publication = spy  # type: ignore[assignment]
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)

            b = sparse_history["batch"]
            # Exactly one publication covered the batch's full global extent...
            batch_pubs = [o for o in observed if o[0] == b["start_seq"]]
            assert len(batch_pubs) == 1
            assert batch_pubs[0][1] == b["end_seq"]
            # ...and by then A001 already carried the batch's subscribed value,
            # proving the unsubscribed majority of the batch was not required.
            assert batch_pubs[0][2] == 1250
            assert cat.get_shot("film-sub-a", "A001")["repair_note"] == "cross-batch"
            assert cat.get_shot("film-sub-b", "B001")["duration_ms"] == 3250
    finally:
        httpd.shutdown()


def _write_history(path, records):
    import json as _json
    path.write_text("\n".join(_json.dumps(r, ensure_ascii=False)
                              for r in records) + "\n")


def _upsert(seq, film, shot, ms, batch=None):
    rec = {"seq": seq, "event_id": f"e{seq}", "event_type": "upsert",
           "film_id": film, "shot_id": shot, "version": 2,
           "ts": "2026-11-01T00:00:00Z", "payload": {"duration_ms": ms}}
    if batch:
        rec["batch_id"] = batch
    return rec


def test_batch_starting_with_unsubscribed_film_publishes_subscribed_survivors(tmp_path):
    """A batch whose FIRST record is unsubscribed still publishes atomically.

    The whole extent is learned from the page metadata, so the subscribed
    survivors become visible in one group even though the batch begins on a
    film we never transfer.
    """
    recs = [_upsert(i + 1, "film-sub", f"S{i:03d}", 100) for i in range(10)]
    batch_start = 11
    recs += [
        _upsert(11, "film-skip", "X001", 500, batch="BX"),   # unsubscribed first
        _upsert(12, "film-sub", "S000", 555, batch="BX"),
        _upsert(13, "film-skip", "X002", 500, batch="BX"),   # unsubscribed middle
        _upsert(14, "film-sub", "S001", 666, batch="BX"),
        _upsert(15, "film-skip", "X001", 777),               # trailing unsubscribed
        _upsert(16, "film-sub", "S002", 888),                # trailing subscribed
    ]
    path = tmp_path / "batchfirst.jsonl"
    _write_history(path, recs)
    total = 16
    source, httpd, url = _serve(str(path), page_size=3)
    try:
        with Catalog(tmp_path / "batchfirst.duckdb") as cat:
            cat.add_subscription("film-sub")
            eng = SyncEngine(cat, LogSourceClient(url), page_size=3,
                             poll_interval=0.0, sparse_scan_span=3)
            eng.run_until_caught_up(max_passes=200, idle_passes_to_stop=1)
            assert cat.last_snapshot_seq() == total
            assert cat.get_shot("film-sub", "S000")["duration_ms"] == 555
            assert cat.get_shot("film-sub", "S001")["duration_ms"] == 666
            assert cat.get_shot("film-sub", "S002")["duration_ms"] == 888
            assert cat.get_shot("film-skip", "X001") is None
            # The batch was published as a single atomic group of its full extent.
            bg = [h for h in eng.counters.history if h["batch_id"] == "BX"]
            assert len(bg) == 1 and bg[0]["start_seq"] == batch_start
            assert bg[0]["end_seq"] == 14
            assert cat.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


def test_batch_straddling_scan_windows_commits_as_one_group(tmp_path):
    """A batch spanning several scan windows is still one atomic group."""
    recs = [_upsert(i + 1, "film-s", f"S{i:03d}", 100) for i in range(37)]
    batch_start = 38
    recs += [_upsert(38 + i, "film-s", f"S{i:03d}", 200, batch="BX")
             for i in range(30)]
    batch_end = 67
    recs += [_upsert(68 + i, "film-s", f"S{i % 37:03d}", 300) for i in range(20)]
    path = tmp_path / "straddle_window.jsonl"
    _write_history(path, recs)
    total = 87
    source, httpd, url = _serve(str(path), page_size=7)
    try:
        with Catalog(tmp_path / "straddle_window.duckdb") as cat:
            cat.add_subscription("film-s")
            eng = SyncEngine(cat, LogSourceClient(url), page_size=7,
                             poll_interval=0.0, sparse_scan_span=7)
            eng.run_until_caught_up(max_passes=300, idle_passes_to_stop=1)
            assert cat.last_snapshot_seq() == total
            assert cat.rebuild_mismatch_count() == 0
            bg = [h for h in eng.counters.history if h["batch_id"] == "BX"]
            assert len(bg) == 1
            assert (bg[0]["start_seq"], bg[0]["end_seq"]) == (batch_start, batch_end)
    finally:
        httpd.shutdown()


def test_append_subscription_backfills_only_the_new_film(sparse_history):
    source, httpd, url = _serve(sparse_history["path"])
    try:
        with Catalog(sparse_history["path"] + ".duckdb") as cat:
            cat.add_subscription("film-sub-a")
            eng = _engine(cat, url)
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
            snap_before = cat.last_snapshot_seq()
            a_before = cat.get_shot("film-sub-a", "A001")
            staged_before = cat.stats()["staged_events"]

            # Append a late film: it must not disturb the active subscription.
            assert cat.add_subscription("film-late") == "backfill"
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)

            # The new film is queryable and correct...
            late = cat.get_shot("film-late", "L001")
            assert late is not None and late["duration_ms"] == 800
            assert cat.subscription_status("film-late") == "active"
            # ...at the same published version the old subscription exposes...
            assert cat.last_snapshot_seq() >= snap_before
            assert cat.get_shot("film-sub-a", "A001") == a_before
            # ...without re-transferring the old subscription's history.
            assert eng.counters.films_activated == 1
            assert cat.stats()["staged_events"] > staged_before
            assert cat.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


def test_backfill_interrupt_resumes_from_durable_progress(sparse_history, tmp_path):
    """Kill mid-backfill, reopen, and finish without redoing completed work."""
    source, httpd, url = _serve(sparse_history["path"])
    db = tmp_path / "resume.duckdb"
    try:
        with Catalog(db) as cat:
            cat.add_subscription("film-sub-a")
            eng = _engine(cat, url)
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
            cat.add_subscription("film-late")

            # Advance backfill part of the way, then "crash".
            _, folded = cat.backfill_progress("film-late")
            eng._backfill_walk("film-late", folded)
            # Pretend a hard kill happened *before* activation.
            progressed = cat.backfill_progress("film-late")[1]
            cat._conn.execute(
                "UPDATE film_subscriptions SET status = 'backfill' "
                "WHERE film_id = 'film-late'"
            )
            assert cat.subscription_status("film-late") == "backfill"

        # A brand-new Catalog over the same file sees the durable progress.
        with Catalog(db) as cat2:
            assert cat2.subscription_status("film-late") == "backfill"
            resumed_from = cat2.backfill_progress("film-late")[1]
            assert resumed_from == progressed  # progress survived the crash
            eng2 = _engine(cat2, url)
            eng2.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
            assert cat2.subscription_status("film-late") == "active"
            assert cat2.get_shot("film-late", "L002")["duration_ms"] == 900
            assert cat2.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


def test_new_film_hidden_until_backfill_completes(sparse_history):
    """A subscribed-but-still-backfilling film is never queryable early."""
    source, httpd, url = _serve(sparse_history["path"])
    try:
        with Catalog(sparse_history["path"] + ".duckdb") as cat:
            cat.add_subscription("film-sub-a")
            eng = _engine(cat, url)
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
            cat.add_subscription("film-late")
            # Stage the late film's records and fold only up to its first seq,
            # so L001 exists in state but the film is still mid-backfill.
            client = LogSourceClient(url)
            raw = client.fetch_page(0, 1000, films=["film-late"])["records"]
            cat.stage_records([normalize(r) for r in raw])
            cat.fold_film_range("film-late", 0,
                                sparse_history["late"]["start_seq"])
            assert cat.subscription_status("film-late") == "backfill"
            assert cat.get_shot("film-late", "L001") is None
            assert cat.list_film_shots("film-late")["count"] == 0
            # Finishing the backfill makes it visible.
            eng.run_until_caught_up(max_passes=100, idle_passes_to_stop=1)
            assert cat.subscription_status("film-late") == "active"
            assert cat.get_shot("film-late", "L001") is not None
    finally:
        httpd.shutdown()


def test_backfill_of_film_with_events_straddling_cutover(tmp_path):
    """A film with history before AND after the cutover backfills correctly.

    The film is subscribed mid-stream: its pre-cutover revisions must be folded
    during backfill, and its post-cutover revisions must then flow through the
    ordinary global publication -- ending at the true final state, not a stale
    snapshot of the cutover.
    """
    import json as _json

    seq = 1
    lines = []

    def rec(event_type, film, shot, version, payload):
        nonlocal seq
        lines.append(_json.dumps({
            "seq": seq, "event_id": f"e{seq}", "event_type": event_type,
            "film_id": film, "shot_id": shot, "version": version,
            "ts": "2026-11-01T00:00:00Z", "payload": payload,
        }, ensure_ascii=False))
        seq += 1

    for i in range(20):
        rec("upsert", "film-keep", f"K{i:02d}", 2, {"duration_ms": 1000 + i})
    rec("upsert", "film-new", "N01", 2, {"duration_ms": 5000, "title": "v1"})
    for i in range(20):
        rec("upsert", "film-keep", f"K{i:02d}", 2, {"duration_ms": 2000 + i})
    rec("upsert", "film-new", "N01", 2, {"duration_ms": 5500, "title": "v2"})
    rec("upsert", "film-new", "N02", 2, {"duration_ms": 6600})
    cutover = seq - 1  # everything <= cutover predates the film-new subscription
    for i in range(20):
        rec("upsert", "film-keep", f"K{i:02d}", 2, {"duration_ms": 3000 + i})
    rec("upsert", "film-new", "N01", 2, {"duration_ms": 5900,
                                         "repair_note": "late"})
    for i in range(10):
        rec("upsert", "film-keep", f"K{i:02d}", 2, {"duration_ms": 4000 + i})

    path = tmp_path / "straddle.jsonl"
    path.write_text("\n".join(lines) + "\n")
    total = seq - 1

    source, httpd, url = _serve(str(path), page_size=10)
    try:
        with Catalog(tmp_path / "straddle.duckdb") as cat:
            cat.add_subscription("film-keep")
            eng = _engine(cat, url, page_size=10)
            # Publish only the pre-cutover prefix first.
            cat.mark_resolved(cutover)
            eng._commit_ready_groups()
            assert cat.last_snapshot_seq() == cutover

            # Subscribe film-new mid-stream and backfill it: at the cutover it
            # reflects the v2 state and is not yet queryable.
            assert cat.add_subscription("film-new") == "backfill"
            assert cat.get_shot("film-new", "N01") is None
            eng._backfill_step()
            assert cat.subscription_status("film-new") == "active"
            n01 = cat.get_shot("film-new", "N01")
            assert n01["duration_ms"] == 5500 and n01["title"] == "v2"
            assert n01["repair_note"] is None  # post-cutover revision not yet out

            # Advance the whole library; the already-active film and the new
            # film both reach the true final state.
            eng.run_until_caught_up(max_passes=120, idle_passes_to_stop=1)
            assert cat.last_snapshot_seq() == total
            n01 = cat.get_shot("film-new", "N01")
            assert (n01["duration_ms"], n01["title"], n01["repair_note"]) == \
                (5900, "v2", "late")
            assert cat.get_shot("film-new", "N02")["duration_ms"] == 6600
            assert cat.get_shot("film-keep", "K00")["duration_ms"] == 4000
            assert cat.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


def test_messy_delivery_still_correct_in_sparse_mode(sparse_history):
    """Shuffle, duplicates and 500s are tolerated while filtering by film."""
    source, httpd, url = _serve(sparse_history["path"], shuffle=True, dup=0.4,
                                err=0.25, seed=77)
    try:
        with Catalog(sparse_history["path"] + ".duckdb") as cat:
            cat.add_subscription("film-sub-a")
            cat.add_subscription("film-sub-b")
            cat.add_subscription("film-late")
            # A small scan span forces many requests, guaranteeing some hit the
            # simulated transient failures.
            eng = SyncEngine(cat, LogSourceClient(url), page_size=50,
                             poll_interval=0.0, sparse_scan_span=50)
            eng.run_until_caught_up(max_passes=150, idle_passes_to_stop=1)
            assert eng.counters.transient_errors > 0  # errors really happened
            for film, shots in sparse_history["expected"].items():
                got = {s["shot_id"]: s for s in cat.list_film_shots(film)["shots"]}
                for shot, exp in shots.items():
                    for attr, val in exp.items():
                        assert got[shot][attr] == val, (film, shot, attr)
            assert cat.get_shot("film-skip", "SK001") is None
            assert cat.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()
