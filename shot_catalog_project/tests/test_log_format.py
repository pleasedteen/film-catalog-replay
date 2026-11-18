"""Tests for log record validation and v1/v2 normalization."""

import pytest

from shot_catalog.log_format import LogFormatError, normalize


def test_v1_duration_seconds_convert_to_milliseconds():
    rec = normalize({
        "seq": 1, "event_id": "e1", "event_type": "upsert",
        "film_id": "f", "shot_id": "s", "version": 1, "ts": "2026-11-01T00:00:01Z",
        "payload": {"duration_sec": 12.5, "title": "t"},
    })
    assert rec.duration_ms == 12500
    assert rec.duration_ms_present is True
    assert rec.title == "t"
    assert rec.repair_note_present is False  # v1 cannot carry repair notes


def test_v2_milliseconds_kept_as_is():
    rec = normalize({
        "seq": 2, "event_id": "e2", "event_type": "upsert",
        "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
        "payload": {"duration_ms": 8400, "repair_note": "fixed"},
    })
    assert rec.duration_ms == 8400
    assert rec.repair_note == "fixed"
    assert rec.repair_note_present is True


def test_partial_revision_omits_other_attributes():
    rec = normalize({
        "seq": 3, "event_id": "e3", "event_type": "upsert",
        "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
        "payload": {"title": "new title"},
    })
    assert rec.title_present is True
    assert rec.duration_ms_present is False
    assert rec.description_present is False
    assert rec.repair_note_present is False


def test_explicit_null_clears_but_still_present():
    rec = normalize({
        "seq": 4, "event_id": "e4", "event_type": "upsert",
        "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
        "payload": {"description": None, "repair_note": None},
    })
    assert rec.description is None and rec.description_present is True
    assert rec.repair_note is None and rec.repair_note_present is True


def test_v1_reject_milliseconds_and_repair_note():
    with pytest.raises(LogFormatError):
        normalize({"seq": 1, "event_id": "e", "event_type": "upsert",
                   "film_id": "f", "shot_id": "s", "version": 1, "ts": "ts",
                   "payload": {"duration_ms": 100}})
    with pytest.raises(LogFormatError):
        normalize({"seq": 1, "event_id": "e", "event_type": "upsert",
                   "film_id": "f", "shot_id": "s", "version": 1, "ts": "ts",
                   "payload": {"duration_sec": 1, "repair_note": "x"}})


def test_v2_reject_seconds():
    with pytest.raises(LogFormatError):
        normalize({"seq": 1, "event_id": "e", "event_type": "upsert",
                   "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
                   "payload": {"duration_sec": 1}})


def test_withdraw_and_republish_have_no_payload():
    for event in ("withdraw", "republish"):
        rec = normalize({"seq": 1, "event_id": "e", "event_type": event,
                         "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts"})
        assert rec.event_type == event
        assert rec.duration_ms_present is False
        with pytest.raises(LogFormatError):
            normalize({"seq": 1, "event_id": "e", "event_type": event,
                       "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
                       "payload": {"x": 1}})


@pytest.mark.parametrize("bad", [
    {"seq": 0, "event_id": "e", "event_type": "upsert", "film_id": "f",
     "shot_id": "s", "version": 1, "ts": "ts", "payload": {"duration_sec": 1}},
    {"seq": 1, "event_id": "e", "event_type": "explode", "film_id": "f",
     "shot_id": "s", "version": 1, "ts": "ts"},
    {"seq": 1, "event_id": "e", "event_type": "upsert", "film_id": "f",
     "shot_id": "s", "version": 9, "ts": "ts"},
])
def test_malformed_records_rejected(bad):
    with pytest.raises(LogFormatError):
        normalize(bad)


def test_batch_id_optional_but_nonempty():
    rec = normalize({"seq": 1, "event_id": "e", "event_type": "withdraw",
                     "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts"})
    assert rec.batch_id is None
    rec = normalize({"seq": 1, "event_id": "e", "event_type": "withdraw",
                     "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
                     "batch_id": "B1"})
    assert rec.batch_id == "B1"
    with pytest.raises(LogFormatError):
        normalize({"seq": 1, "event_id": "e", "event_type": "withdraw",
                   "film_id": "f", "shot_id": "s", "version": 2, "ts": "ts",
                   "batch_id": ""})
