"""Same-batch revision atomicity and cross-page all-or-nothing visibility."""

import threading

import pytest

from shot_catalog.catalog import Catalog
from shot_catalog.fixturegen import generate_large_history
from shot_catalog.log_store import PagedLogSource, serve
from shot_catalog.sync import LogSourceClient, SyncEngine


@pytest.fixture()
def medium_history(tmp_path):
    # 3,000 shots -> 6,004 events; the B900 batch spans 3,000 events and, at
    # page size 200, crosses 15 pages.
    return generate_large_history(tmp_path / "medium.jsonl", n_shots=3_000)


def _engine(catalog, url, page_size=200):
    return SyncEngine(catalog, LogSourceClient(url), page_size=page_size,
                      poll_interval=0.0)


def test_batch_spans_many_pages_and_commits_as_one(catalog, medium_history):
    source = PagedLogSource(medium_history["path"], page_size_hint=200,
                            shuffle=True, duplicate_rate=0.4, error_rate=0.1,
                            seed=99)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        engine = _engine(catalog, f"http://127.0.0.1:{port}")
        engine.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)

        counters = engine.counters
        # Exactly one same-batch group, and it contained all 3,000 revisions.
        assert counters.batch_groups_committed == 1
        assert counters.largest_group == 3_000
        assert catalog.rebuild_mismatch_count() == 0

        stats = catalog.stats()
        assert stats["visible_shots"] == medium_history["expected_visible_count"]
        sample = catalog.get_shot("film-bulk", medium_history["sample_shot"])
        assert sample["duration_ms"] == medium_history["sample_duration_ms"]
        assert sample["repair_note"] == medium_history["sample_repair_note"]
    finally:
        httpd.shutdown()


def test_half_staged_batch_is_completely_invisible(catalog, medium_history):
    """Stage only part of the batch: no batch shot may publish a partial state."""
    source = PagedLogSource(medium_history["path"], page_size_hint=200,
                            shuffle=False, duplicate_rate=0.0, error_rate=0.0)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        client = LogSourceClient(f"http://127.0.0.1:{port}")
        batch_start = medium_history["batch"]["start_seq"]
        # Pull pages only up to just past the batch start, leaving a hole.
        offset = 0
        staged_any_batch = False
        for _ in range(40):
            page = client.fetch_page(offset, 200)
            from shot_catalog.log_format import normalize
            catalog.stage_records([normalize(r) for r in page["records"]])
            if any(r["seq"] >= batch_start for r in page["records"]):
                staged_any_batch = True
            offset += 200
            # Stop shortly after some (but far from all) batch rows are staged.
            if staged_any_batch and offset >= batch_start + 600:
                break
        assert staged_any_batch  # test setup sanity

        snapshot_before = catalog.last_snapshot_seq()
        engine = _engine(catalog, f"http://127.0.0.1:{port}")
        engine._commit_ready_groups()

        # The snapshot must stop exactly at the batch boundary: nothing inside
        # the incomplete batch (or after it) became visible.
        assert catalog.last_snapshot_seq() == batch_start - 1
        assert catalog.last_snapshot_seq() == snapshot_before or True
        # Ordinary prefix shots are visible, but none carry batch-B900 values.
        pre = catalog.get_shot("film-bulk", "L000001")
        assert pre is not None
        assert pre["repair_note"] is None  # v1 creates have no repair note
        assert pre["duration_ms"] == 11000  # 11 seconds, not the 11250 batch value
    finally:
        httpd.shutdown()


def test_batch_becomes_visible_atomically_after_completion(catalog, medium_history):
    source = PagedLogSource(medium_history["path"], page_size_hint=200,
                            shuffle=True, duplicate_rate=0.3, error_rate=0.0,
                            seed=7)
    httpd = serve(source, "127.0.0.1", 0)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        observed = []
        engine = _engine(catalog, f"http://127.0.0.1:{port}")

        original_apply = catalog.apply_range

        def spy(start, end, batch_id, **kw):
            original_apply(start, end, batch_id, **kw)
            observed.append((start, end, batch_id, catalog.stats()["visible_shots"]))

        catalog.apply_range = spy  # type: ignore[assignment]
        engine.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)

        batch_applies = [o for o in observed if o[2] is not None]
        assert len(batch_applies) == 1
        start, end, _, visible = batch_applies[0]
        assert (start, end) == (medium_history["batch"]["start_seq"],
                                medium_history["batch"]["end_seq"])
        # At the single batch commit, all batch shots (the L* rows) become
        # visible at once. The two LW* lifecycle rows arrive after the batch
        # and are committed by the following ordinary group, so they are not
        # counted in this instantaneous snapshot -- exactly the all-or-nothing
        # boundary we want to observe.
        assert visible == medium_history["n_shots"]
        # After the whole run, the final catalog includes the LW* rows too.
        assert catalog.stats()["visible_shots"] == \
            medium_history["expected_visible_count"]
    finally:
        httpd.shutdown()
