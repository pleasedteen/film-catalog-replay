"""Hot -> cold archival semantics, outage backpressure and recovery.

Covers the cold-storage requirements:

* migration keeps the hot database directory layout and the published water
  mark unchanged; unpublished staged data can still be committed; the catalog
  stays queryable throughout;
* the cold side is an independent Parquet directory with a manifest and a
  state table that reconciles orphans/tmp files and missing segments;
* during a cold outage online queries keep working, at most ONE new published
  commit group accumulates, paging pauses, and recovery auto-catches up;
* redelivery of archived (old) pages only touches the hot database;
* with the log source gone, the catalog is rebuilt and verified from the
  local history alone;
* a real SIGKILL landing mid-archival resumes coherently;
* an old hot-only database accepts a cold directory later without migration
  pain (legacy continuity).
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from shot_catalog.catalog import Catalog
from shot_catalog.cold import ColdStorage, ColdUnavailable
from shot_catalog.fixturegen import generate_mixed_history
from shot_catalog.log_format import normalize
from shot_catalog.log_store import PagedLogSource, serve
from shot_catalog.server import build_handler
from shot_catalog.sync import LogSourceClient, SyncEngine, make_archiver


def _start_log_server(history_path, *, page_size=5, seed=7, error_rate=0.15):
    source = PagedLogSource(history_path, page_size_hint=page_size,
                            shuffle=True, duplicate_rate=0.35,
                            error_rate=error_rate, seed=seed)
    httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _start_query_server(catalog):
    httpd = ThreadingHTTPServer(
        ("127.0.0.1", 0), build_handler(catalog))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _get(base, path):
    try:
        with urllib.request.urlopen(base + path, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


# ----------------------------------------------------------- happy-path archival
def test_archive_keeps_watermark_directory_and_queries(tmp_path, mixed_summary):
    """Archival moves rows only below the published watermark; everything works."""
    httpd, url = _start_log_server(mixed_summary["path"])
    db = tmp_path / "cat.duckdb"
    cold_dir = tmp_path / "cold"
    try:
        with Catalog(db, cold=ColdStorage(cold_dir)) as cat:
            eng = SyncEngine(
                cat, LogSourceClient(url), page_size=5, poll_interval=0.0,
                archiver=make_archiver(cat, target_rows=6),
            )
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)

            # The published watermark still covers the whole source history...
            assert cat.last_snapshot_seq() == mixed_summary["total_events"]
            # ...while some history has physically moved to cold segments.
            stats = cat.stats()
            assert stats["cold_segments"] >= 2
            assert stats["cold_events"] > 0
            assert stats["staged_events"] < mixed_summary["total_events"]
            assert stats["archived_through_seq"] > 0
            # segment seq ranges are gapless and never exceed the watermark
            segs = cat._committed_segments()
            assert segs[0].start_seq == 1
            assert segs[-1].end_seq <= cat.last_snapshot_seq()
            for prev, nxt in zip(segs, segs[1:]):
                assert nxt.start_seq == prev.end_seq + 1
                assert nxt.row_count == nxt.end_seq - nxt.start_seq + 1
            # the hot DuckDB file keeps exactly the same tables/directory role
            names = {r[0] for r in
                     cat._conn.execute("SHOW TABLES").fetchall()}
            assert {"event_staging", "shot_state", "sync_state",
                    "catalog_current", "cold_archive_state"} <= names
            # catalog answers are identical to the fixture expectations
            assert cat.rebuild_mismatch_count() == 0
            assert cat.verify_full_history_oracle() == 0
            for film, shots in mixed_summary["expected"].items():
                got = {s["shot_id"]: s
                       for s in cat.list_film_shots(film)["shots"]}
                assert set(got) == set(shots)
            # the cold directory is self-contained: manifest + parquet only
            cold_files = {p.name for p in cold_dir.rglob("*") if p.is_file()}
            assert "manifest.json" in cold_files
            assert all(n.endswith(".parquet")
                       for n in cold_files if n != "manifest.json")
            manifest = json.loads(
                (cold_dir / "manifest.json").read_text(encoding="utf-8"))
            assert manifest["segment_count"] == stats["cold_segments"]
            assert manifest["archived_through_seq"] == \
                stats["archived_through_seq"]
    finally:
        httpd.shutdown()


def test_unpublished_staged_data_is_never_archived(tmp_path, mixed_summary):
    """A staged-ahead hole stays in hot storage until it is published."""
    httpd, url = _start_log_server(mixed_summary["path"], error_rate=0.0)
    db = tmp_path / "cat.duckdb"
    try:
        with Catalog(db, cold=ColdStorage(tmp_path / "cold")) as cat:
            client = LogSourceClient(url)
            # Stage the whole history first, but publish only a prefix.
            offset = 0
            while True:
                page = client.fetch_page(offset, 5)
                cat.stage_records([normalize(r) for r in page["records"]])
                if not page["has_more"]:
                    break
                offset += 5
            eng = SyncEngine(cat, client, page_size=5, poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=4))
            # Publish just two coalesced groups so a real prefix exists.
            eng._commit_ready_groups(ordinary_flush_size=4, max_groups=2)
            watermark = cat.last_snapshot_seq()
            assert 0 < watermark < mixed_summary["total_events"]
            # Drain the archiver: it may move only rows <= watermark.
            cat.drain_hot_to_cold(target_rows=4)
            assert cat.archived_through_seq() <= watermark
            # Staged-but-unpublished rows are still all in the hot table.
            hot_after = cat._conn.execute(
                "SELECT count(*) FROM event_staging WHERE seq > ?",
                [watermark]).fetchone()[0]
            assert hot_after == mixed_summary["total_events"] - watermark
            # Now finish publishing; the rest becomes archivable and the
            # whole history still verifies.
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            cat.drain_hot_to_cold(target_rows=4)
            assert cat.archived_through_seq() == mixed_summary["total_events"]
            assert cat.verify_full_history_oracle() == 0
    finally:
        httpd.shutdown()


def test_batch_group_is_never_split_across_segments(tmp_path):
    """A same-batch run moves to cold as one unit (no half-batch segment)."""
    from shot_catalog.fixturegen import generate_large_history

    history = generate_large_history(tmp_path / "h.jsonl", n_shots=400)
    httpd, url = _start_log_server(history["path"], page_size=100, seed=3,
                                   error_rate=0.0)
    try:
        with Catalog(tmp_path / "cat.duckdb",
                     cold=ColdStorage(tmp_path / "cold")) as cat:
            eng = SyncEngine(cat, LogSourceClient(url), page_size=100,
                             poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=120))
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            cat.drain_hot_to_cold(target_rows=120)
            bstart, bend = (history["batch"]["start_seq"],
                            history["batch"]["end_seq"])
            for seg in cat._committed_segments():
                covers_batch = seg.start_seq <= bstart <= seg.end_seq
                if covers_batch:
                    assert seg.start_seq == bstart
                    assert seg.end_seq == bend
                    break
            else:  # pragma: no cover - batch must live in some segment
                pytest.fail("batch run not found in any cold segment")
            assert cat.verify_full_history_oracle() == 0
    finally:
        httpd.shutdown()


# ------------------------------------------------------------- outage behavior
def test_cold_outage_keeps_queries_and_bounds_backlog_to_one_group(
        tmp_path, mixed_summary):
    cold = ColdStorage(tmp_path / "cold")
    httpd, url = _start_log_server(mixed_summary["path"])
    try:
        with Catalog(tmp_path / "cat.duckdb", cold=cold) as cat:
            eng = SyncEngine(
                cat, LogSourceClient(url), page_size=2, poll_interval=0.0,
                archiver=make_archiver(cat, target_rows=4),
            )
            # Stage the first page but do not publish anything yet.
            first_page = eng.client.fetch_page(0, 2)
            cat.stage_records([normalize(r) for r in first_page["records"]])
            cat.record_fetch_watermark(2)
            eng.counters.pages_fetched += 1
            assert cat.last_snapshot_seq() == 0
            cold.set_offline()
            assert eng.cold_paused() is False  # nothing published yet

            query_httpd, qbase = _start_query_server(cat)
            try:
                # The first staged group may still publish (at-most-one
                # allowance); its archival fails and becomes the one backlog.
                eng._commit_ready_groups(max_groups=1)
                assert cat.last_snapshot_seq() > 0
                assert cat.hot_published_group_count() == 1
                assert eng.counters.cold_outages == 1

                # From then on no further group may publish and no further
                # source page may be fetched while the cold side is down.
                before_snap = cat.last_snapshot_seq()
                pages_now = eng.counters.pages_fetched
                eng.run_until_caught_up(max_passes=10, idle_passes_to_stop=1)
                assert cat.last_snapshot_seq() == before_snap
                assert cat.last_snapshot_seq() < mixed_summary["total_events"]
                assert cat.hot_published_group_count() == 1
                assert eng.counters.cold_pause_events >= 1
                assert eng.counters.pages_fetched == pages_now
                # Online queries never depend on the cold side. The snapshot
                # only covers the first group here, so assert a coherent
                # answer rather than the end-of-history batch value.
                code, shot = _get(qbase, "/shot?film_id=film-a&shot_id=S001")
                assert code == 200 and isinstance(shot["duration_ms"], int)
                code, status = _get(qbase, "/cold/status")
                assert code == 200 and status["available"] is False
                assert status["hot_published_backlog_groups"] == 1
                code, health = _get(qbase, "/health")
                assert code == 200 and health["cold_available"] is False
                # A further poll while paused must not page any source data.
                eng._pull_and_stage(start_offset=eng._resume_offset())
                assert eng.counters.pages_fetched == pages_now
                assert eng.cold_paused() is True
            finally:
                query_httpd.shutdown()

            # Recovery: backlog drains, then full catch-up completes.
            cold.set_online()
            assert eng.cold_paused() is False
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            cat.drain_hot_to_cold(target_rows=4)
            assert cat.last_snapshot_seq() == mixed_summary["total_events"]
            assert cat.hot_published_group_count() == 0
            assert cat.verify_full_history_oracle() == 0
            assert cat.stats()["cold_available"] is True
    finally:
        httpd.shutdown()


def test_redelivery_of_archived_pages_is_hot_only_and_harmless(
        tmp_path, mixed_summary):
    httpd, url = _start_log_server(mixed_summary["path"], error_rate=0.0)
    try:
        with Catalog(tmp_path / "cat.duckdb",
                     cold=ColdStorage(tmp_path / "cold")) as cat:
            eng = SyncEngine(cat, LogSourceClient(url), page_size=5,
                             poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=4))
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            cat.drain_hot_to_cold(target_rows=4)
            archived = cat.archived_through_seq()
            assert archived > 2

            # Re-fetch an OLD page whose seqs are already archived.
            page = LogSourceClient(url).fetch_page(0, 5)
            assert any(r["seq"] <= archived for r in page["records"])
            seg_names_before = {
                p.name for p in (tmp_path / "cold" / "segments").iterdir()}
            hot_before = cat.stats()["staged_events"]
            newly = cat.stage_records([normalize(r) for r in page["records"]])
            # The redelivered rows land back in HOT staging (hot-only path);
            # cold segments are never rewritten and the archival boundary is
            # unchanged while the shadow rows sit in the hot table.
            assert newly == 5
            assert cat.archived_through_seq() == archived
            assert cat.stats()["staged_events"] >= 5
            assert cat.verify_full_history_oracle() == 0
            assert cat.rebuild_mismatch_count() == 0
            # Reclaiming verifies content against the Parquet copies and
            # removes the shadow rows without creating duplicate segments.
            reclaimed = cat._reclaim_shadowed_hot_rows()
            assert reclaimed == 5
            assert cat.stats()["staged_events"] == hot_before
            seg_names_after = {
                p.name for p in (tmp_path / "cold" / "segments").iterdir()}
            assert seg_names_after == seg_names_before
            assert cat.verify_full_history_oracle() == 0
    finally:
        httpd.shutdown()


# ------------------------------------------------- rebuild without the log source
def test_rebuild_and_verify_from_local_history_without_source(tmp_path,
                                                              mixed_summary):
    """After the log source is gone, local history rebuilds the staging view."""
    httpd, url = _start_log_server(mixed_summary["path"])
    jsonl_export = tmp_path / "local_history.jsonl"
    try:
        with Catalog(tmp_path / "cat.duckdb",
                     cold=ColdStorage(tmp_path / "cold")) as cat:
            eng = SyncEngine(cat, LogSourceClient(url), page_size=5,
                             poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=5))
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            cat.drain_hot_to_cold(target_rows=5)
            # Snapshot every answer for comparison after the source is gone.
            expected = {
                film: cat.list_film_shots(film)
                for film in mixed_summary["films"]}
            exported = cat.export_history_jsonl(jsonl_export)
            assert exported == mixed_summary["total_events"]
    finally:
        httpd.shutdown()  # log source is now closed

    # A brand-new database, no HTTP anywhere: rebuild purely from local files.
    rebuilt_db = tmp_path / "rebuilt.duckdb"
    with Catalog(rebuilt_db, cold=ColdStorage(tmp_path / "rebuilt_cold")) \
            as rebuilt:
        loaded = rebuilt.load_jsonl_history(jsonl_export)
        assert loaded == mixed_summary["total_events"]
        # Replay the same commit-group machinery over the local history.
        class _LocalClient:  # tiny stand-in: total() only, never fetched
            def total(self):
                return mixed_summary["total_events"]

        eng = SyncEngine(rebuilt, _LocalClient(), page_size=5,
                         poll_interval=0.0)
        eng._commit_ready_groups()
        assert rebuilt.last_snapshot_seq() == mixed_summary["total_events"]
        assert rebuilt.rebuild_mismatch_count() == 0
        for film, listing in expected.items():
            got = rebuilt.list_film_shots(film)
            assert got["shots"] == listing["shots"]

    # And the ORIGINAL catalog still verifies against its own cold+hot history
    # with no source available.
    with Catalog(tmp_path / "cat.duckdb",
                 cold=ColdStorage(tmp_path / "cold")) as cat:
        assert cat.verify_full_history_oracle() == 0
        assert jsonl_export.read_text(encoding="utf-8").count("\n") == \
            mixed_summary["total_events"]


# --------------------------------------------------------- manifest reconciliation
def test_reconcile_removes_orphans_and_regenerates_manifest(tmp_path,
                                                            mixed_summary):
    httpd, url = _start_log_server(mixed_summary["path"])
    cold_dir = tmp_path / "cold"
    try:
        with Catalog(tmp_path / "cat.duckdb", cold=ColdStorage(cold_dir)) \
                as cat:
            eng = SyncEngine(cat, LogSourceClient(url), page_size=5,
                             poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=6))
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            assert len(cat._committed_segments()) >= 2

        seg_dir = cold_dir / "segments"
        # Simulate crash leftovers: a tmp export and an orphaned parquet.
        (seg_dir / "seg-00000099.parquet.tmp").write_bytes(b"partial")
        (seg_dir / "seg-00000098.parquet").write_bytes(b"orphan")
        (cold_dir / "manifest.json").unlink()

        with Catalog(tmp_path / "cat.duckdb", cold=ColdStorage(cold_dir)) \
                as cat:
            names = {p.name for p in seg_dir.iterdir()}
            assert "seg-00000099.parquet.tmp" not in names
            assert "seg-00000098.parquet" not in names
            manifest = json.loads(
                (cold_dir / "manifest.json").read_text(encoding="utf-8"))
            assert manifest["segment_count"] == cat.stats()["cold_segments"]
            assert cat.verify_full_history_oracle() == 0
    finally:
        httpd.shutdown()


def test_missing_committed_segment_takes_cold_offline_but_queries_survive(
        tmp_path, mixed_summary):
    httpd, url = _start_log_server(mixed_summary["path"])
    cold_dir = tmp_path / "cold"
    try:
        with Catalog(tmp_path / "cat.duckdb", cold=ColdStorage(cold_dir)) \
                as cat:
            eng = SyncEngine(cat, LogSourceClient(url), page_size=5,
                             poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=6))
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            victim = cat._committed_segments()[0]

        victim_path = cold_dir / "segments" / victim.file_name
        backup = tmp_path / victim.file_name
        victim_path.replace(backup)  # the cold mount loses one segment file

        with Catalog(tmp_path / "cat.duckdb", cold=ColdStorage(cold_dir)) \
                as cat:
            assert cat.cold_available() is False
            # Hot-only queries are unaffected by the cold outage.
            assert cat.get_shot("film-a", "S001")["duration_ms"] == 13100
            with pytest.raises(ColdUnavailable):
                cat.verify_full_history_oracle()

        # The mount is repaired and the segment file restored.
        backup.replace(victim_path)
        with Catalog(tmp_path / "cat.duckdb", cold=ColdStorage(cold_dir)) \
                as cat:
            assert cat.cold_available() is True
            assert cat.verify_full_history_oracle() == 0
            assert cat.get_shot("film-b", "S012")["title"] == "备用镜头-启用"
    finally:
        httpd.shutdown()


# ------------------------------------------------------------ real SIGKILL resume
@pytest.mark.skipif(not os.path.exists("/workspace/shot_catalog_project/src"),
                    reason="project layout check")
def test_sigkill_mid_archival_then_restart_resumes(tmp_path, mixed_summary):
    httpd, url = _start_log_server(mixed_summary["path"], seed=55)
    db = tmp_path / "killed.duckdb"
    cold_dir = tmp_path / "cold"
    progress = tmp_path / "progress.txt"
    worker = Path(__file__).resolve().parents[1] / "scripts" / "_cold_worker.py"
    env = dict(os.environ)
    env["PYTHONPATH"] = (
        "/workspace/shot_catalog_project/src:"
        "/workspace/pylibs/lib/python3.11/site-packages")
    env["COLD_DIR"] = str(cold_dir)
    env["ARCHIVE_ROWS"] = "4"
    env["KILL_COLD_OP"] = "segment_rename"
    env["PROGRESS_FILE"] = str(progress)
    try:
        proc = subprocess.Popen(
            [sys.executable, "-I", str(worker), str(db), url, "5", "30"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 30
        killed = False
        while time.time() < deadline:
            if progress.exists():
                try:
                    seq = int(progress.read_text().strip())
                except ValueError:
                    seq = 0
                if seq > 0:
                    killed = True
                    break
            if proc.poll() is not None:
                # The self-inflicted SIGKILL may land just before the poll;
                # give the progress sidecar a brief grace period to appear.
                grace = time.time() + 3
                while time.time() < grace:
                    if progress.exists():
                        killed = True
                        break
                    time.sleep(0.05)
                break
            time.sleep(0.05)
        assert killed, "worker never reached a committable prefix"
        proc.wait(timeout=10)
        assert proc.returncode != 0  # SIGKILL
        prefix = int(progress.read_text().strip())
        assert 0 < prefix <= mixed_summary["total_events"]

        # No tmp/orpan files may survive the restart reconciliation.
        resume_env = {k: v for k, v in env.items()
                      if k not in ("KILL_COLD_OP", "PROGRESS_FILE")}
        proc2 = subprocess.run(
            [sys.executable, "-I", str(worker), str(db), url, "5", "0"],
            env=resume_env, capture_output=True, text=True, timeout=90)
        assert proc2.returncode == 0, proc2.stderr
        leftovers = list((cold_dir / "segments").glob("*.tmp"))
        assert leftovers == []
        with Catalog(db, cold=ColdStorage(cold_dir)) as cat:
            assert cat.last_snapshot_seq() == mixed_summary["total_events"]
            assert cat.verify_full_history_oracle() == 0
            assert cat.rebuild_mismatch_count() == 0
            assert cat.get_shot("film-b", "S012") is not None
    finally:
        httpd.shutdown()


def test_oracle_ignores_staged_ahead_events_when_history_spans_cold(
        tmp_path, mixed_summary):
    """Regression: staged-but-unpublished rows must not count as mismatches."""
    httpd, url = _start_log_server(mixed_summary["path"], error_rate=0.0)
    try:
        with Catalog(tmp_path / "cat.duckdb",
                     cold=ColdStorage(tmp_path / "cold")) as cat:
            client = LogSourceClient(url)
            offset = 0
            while True:
                page = client.fetch_page(offset, 5)
                cat.stage_records([normalize(r) for r in page["records"]])
                if not page["has_more"]:
                    break
                offset += 5
            eng = SyncEngine(cat, client, page_size=5, poll_interval=0.0,
                             archiver=make_archiver(cat, target_rows=4))
            # Publish only part of the staged history and archive it; rows
            # above the watermark remain staged (and partly hot, partly cold).
            eng._commit_ready_groups(ordinary_flush_size=4, max_groups=2)
            cat.drain_hot_to_cold(target_rows=4)
            assert cat.archived_through_seq() > 0
            assert cat.max_staged_seq() > cat.last_snapshot_seq()
            assert cat.verify_full_history_oracle() == 0
            assert cat.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()


# ------------------------------------------------------------- legacy continuity
def test_legacy_hot_only_database_takes_cold_storage_later(tmp_path,
                                                           mixed_summary):
    """An old catalog file (no cold dir) keeps working and can add cold later."""
    httpd, url = _start_log_server(mixed_summary["path"])
    db = tmp_path / "legacy.duckdb"
    try:
        # Phase 1: the pre-cold world -- Catalog with no cold side at all.
        with Catalog(db) as cat:
            eng = SyncEngine(cat, LogSourceClient(url), page_size=5,
                             poll_interval=0.0)
            eng.run_until_caught_up(max_passes=80, idle_passes_to_stop=1)
            assert cat.last_snapshot_seq() == mixed_summary["total_events"]
            assert cat.stats()["cold_enabled"] is False
            assert cat.rebuild_mismatch_count() == 0

        # Phase 2: attach an independent cold directory to the SAME db file.
        with Catalog(db, cold=ColdStorage(tmp_path / "cold")) as cat:
            assert cat.stats()["cold_enabled"] is True
            assert cat.stats()["cold_segments"] == 0
            # Existing hot history is archived without re-reading the source.
            results = cat.drain_hot_to_cold(target_rows=6)
            assert results and cat.archived_through_seq() == \
                mixed_summary["total_events"]
            assert cat.verify_full_history_oracle() == 0
            # New commits keep flowing through the archiver afterward.
            assert cat.get_shot("film-a", "S001")["repair_note"] == \
                "批量校准 13100ms"
    finally:
        httpd.shutdown()
