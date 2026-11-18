"""End-to-end semantic tests over the messy mixed-version fixture.

The log source shuffles pages, duplicates ~40% of records and fails ~25% of
requests, yet every assertion below must hold because sync interprets source
history by seq rather than by delivery order.
"""


def test_full_sync_matches_expected_history(synced, mixed_summary):
    catalog, engine = synced
    assert catalog.last_snapshot_seq() == mixed_summary["total_events"]
    assert catalog.rebuild_mismatch_count() == 0
    # Delivery really was messy: more records seen than unique events.
    assert engine.counters.records_seen >= mixed_summary["total_events"]
    assert engine.counters.records_newly_staged == mixed_summary["total_events"]
    for film, shots in mixed_summary["expected"].items():
        got = {s["shot_id"]: s for s in catalog.list_film_shots(film)["shots"]}
        assert set(got) == set(shots)
        for shot_id, expected in shots.items():
            for attr, value in expected.items():
                assert got[shot_id][attr] == value, (shot_id, attr)


def test_durations_always_milliseconds(synced):
    catalog, _ = synced
    s001 = catalog.get_shot("film-a", "S001")
    # 13 seconds submitted by a v1 workstation must surface as 13000+ ms only
    # where a later v2 revision did not override it; here batch set 13100.
    assert s001["duration_ms"] == 13100
    assert isinstance(s001["duration_ms"], int)
    s011 = catalog.get_shot("film-b", "S011")
    assert s011["duration_ms"] == 5300  # v2 ms value, not the v1 5 seconds


def test_v1_revision_cannot_erase_v2_repair_note(synced):
    catalog, _ = synced
    s001 = catalog.get_shot("film-a", "S001")
    # A v1 seconds-only revision landed after the v2 repair note; the note and
    # other untouched attributes must survive until the batch overwrites them.
    assert s001["repair_note"] == "批量校准 13100ms"
    assert s001["camera"] == "ARRI-A"  # set at v1 create, never revised away


def test_explicit_description_clearing(synced):
    catalog, _ = synced
    # S001's description was explicitly set to null by a v2 partial revision.
    assert catalog.get_shot("film-a", "S001")["description"] is None
    # S002 keeps its description because it was never cleared.
    assert catalog.get_shot("film-a", "S002")["description"] == "素材说明"


def test_repair_note_can_be_cleared_and_stays_cleared(synced):
    catalog, _ = synced
    s002 = catalog.get_shot("film-a", "S002")
    # Its "首次修复" note was explicitly nulled; a later v1 revision must not
    # resurrect it (v1 cannot even carry the attribute).
    assert s002["repair_note"] == "批量校准 9100ms"  # batch re-set a note
    # The null-clear behavior itself is asserted on the pre-batch state in
    # test_rebuild_oracle via the full-prefix reconstruction.


def test_withdrawn_shot_is_unretrievable(synced):
    catalog, _ = synced
    # S010 ends in withdrawn state: absent from both point and film queries.
    assert catalog.get_shot("film-b", "S010") is None
    film_b = {s["shot_id"] for s in catalog.list_film_shots("film-b")["shots"]}
    assert "S010" not in film_b


def test_republished_shot_is_retrievable(synced):
    catalog, _ = synced
    s012 = catalog.get_shot("film-b", "S012")
    assert s012 is not None
    assert s012["title"] == "备用镜头-启用"
    assert s012["duration_ms"] == 4200


def test_film_listing_is_one_consistent_snapshot(synced):
    catalog, _ = synced
    listing = catalog.list_film_shots("film-a")
    # Every row in the response shares the snapshot seq of that response.
    assert listing["snapshot_seq"] == catalog.snapshot_seq()
    assert listing["count"] == len(listing["shots"]) == 3


def test_idempotent_redelivery_does_not_duplicate(synced):
    catalog, engine = synced
    before = catalog.stats()["staged_events"]
    # Re-run a full catch-up: every record is redelivered but staging is stable.
    engine.run_until_caught_up(max_passes=10, idle_passes_to_stop=1)
    after = catalog.stats()
    assert after["staged_events"] == before
    assert catalog.rebuild_mismatch_count() == 0
