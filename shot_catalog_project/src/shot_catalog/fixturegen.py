"""Deterministic source-history fixture generator.

The fixtures are *source commit histories*: gapless ``seq`` values in true
source order. Messy delivery (duplicates, overlapping pages, reordering) is
the log store's job -- see log_store.py.

Two histories are generated:

1. ``mixed_history.jsonl`` -- a small, human-readable history that exercises
   every semantic rule: v1(seconds)/v2(milliseconds) interleaved revisions of
   the same shots, partial revisions, description clearing, withdraw /
   republish, and a multi-page same-batch revision.

2. ``large_history.jsonl`` -- 100,000 shots, each created by a v1
   workstation and later revised inside one same-batch资料修复 spanning
   thousands of log pages (plus withdraw/republish cases). It proves the
   all-or-nothing batch rule at scale without ever loading the batch into
   Python memory.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, IO

_BASE_TS = datetime(2026, 11, 1, 0, 0, 0, tzinfo=timezone.utc)


def _ts(seq: int) -> str:
    return (_BASE_TS + timedelta(seconds=seq)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _rec(seq: int, event_type: str, film: str, shot: str, version: int,
         payload: dict[str, Any] | None = None, batch_id: str | None = None) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "seq": seq,
        "event_id": f"evt-{seq:08d}",
        "event_type": event_type,
        "film_id": film,
        "shot_id": shot,
        "version": version,
        "ts": _ts(seq),
    }
    if batch_id is not None:
        rec["batch_id"] = batch_id
    if payload is not None:
        rec["payload"] = payload
    return rec


def _emit(fh: IO[str], seq: int, *args, **kwargs) -> int:
    fh.write(json.dumps(_rec(seq, *args, **kwargs), ensure_ascii=False, separators=(",", ":")) + "\n")
    return seq + 1


def generate_mixed_history(path: str | Path) -> dict[str, Any]:
    """Write the small mixed-version history. Returns a summary for assertions."""
    path = Path(path)
    seq = 1
    with path.open("w", encoding="utf-8") as fh:
        # --- film-a: shot S001 shows interleaved v1/v2 revisions -------------
        seq = _emit(fh, seq, "upsert", "film-a", "S001", 1,
                    {"title": "开场全景", "description": "初剪说明",
                     "duration_sec": 12, "scene": "SC-01", "camera": "ARRI-A"})
        seq = _emit(fh, seq, "upsert", "film-a", "S001", 2,
                    {"description": "精修说明", "duration_ms": 12500,
                     "repair_note": "修复时间码漂移"})
        # v1 partial revision: only duration in seconds. It must NOT wipe the
        # v2 description or repair_note; duration converts s -> ms.
        seq = _emit(fh, seq, "upsert", "film-a", "S001", 1, {"duration_sec": 13})
        # v2 partial revision clears description only; repair_note survives.
        seq = _emit(fh, seq, "upsert", "film-a", "S001", 2, {"description": None})

        # --- film-a: shot S002 has its repair note cleared explicitly --------
        seq = _emit(fh, seq, "upsert", "film-a", "S002", 2,
                    {"title": "雨中特写", "description": "素材说明",
                     "duration_ms": 8400, "repair_note": "首次修复"})
        seq = _emit(fh, seq, "upsert", "film-a", "S002", 2,
                    {"repair_note": None, "duration_ms": 8600})
        # a later v1 revision cannot resurrect or destroy the (now empty) note
        seq = _emit(fh, seq, "upsert", "film-a", "S002", 1, {"duration_sec": 9})

        # --- film-b: S010 withdraw / republish lifecycle ---------------------
        seq = _emit(fh, seq, "upsert", "film-b", "S010", 1,
                    {"title": "追车镜头", "duration_sec": 20,
                     "description": "动作组初版"})
        seq = _emit(fh, seq, "upsert", "film-b", "S010", 2,
                    {"duration_ms": 20250, "repair_note": "稳定器抖动修复"})
        seq = _emit(fh, seq, "withdraw", "film-b", "S010", 2)
        # a revision arriving while withdrawn still applies to hidden state
        seq = _emit(fh, seq, "upsert", "film-b", "S010", 2,
                    {"title": "追车镜头(重剪)"})
        seq = _emit(fh, seq, "republish", "film-b", "S010", 2)
        seq = _emit(fh, seq, "withdraw", "film-b", "S010", 1)  # withdrawn at end

        # --- film-b: S011 stays visible and is revised while S010 is hidden --
        seq = _emit(fh, seq, "upsert", "film-b", "S011", 1,
                    {"title": "对话正反打", "duration_sec": 5})
        seq = _emit(fh, seq, "upsert", "film-b", "S011", 2,
                    {"duration_ms": 5300, "camera": "SONY-V"})

        # --- film-a: S003 created by v2, then partially revised by v1 --------
        seq = _emit(fh, seq, "upsert", "film-a", "S003", 2,
                    {"title": "夜景航拍", "duration_ms": 17800,
                     "repair_note": "噪点修复"})
        seq = _emit(fh, seq, "upsert", "film-a", "S003", 1,
                    {"description": "航拍组补充说明", "duration_sec": 18})

        # --- multi-page same-batch revision (3 shots, deliberately tiny so a
        # page_size of 2 makes the group span 2 pages; B100-B102 contiguous) --
        batch_start = seq
        for shot, ms in (("S001", 13100), ("S002", 9100), ("S003", 18100)):
            seq = _emit(fh, seq, "upsert", "film-a", shot, 2,
                        {"duration_ms": ms, "repair_note": f"批量校准 {ms}ms"},
                        batch_id="B100")
        batch_end = seq - 1

        # --- film-b S012: withdrawn then republished with brand-new identity --
        seq = _emit(fh, seq, "upsert", "film-b", "S012", 1,
                    {"title": "备用镜头", "duration_sec": 4})
        seq = _emit(fh, seq, "withdraw", "film-b", "S012", 2)
        seq = _emit(fh, seq, "republish", "film-b", "S012", 2)
        seq = _emit(fh, seq, "upsert", "film-b", "S012", 2,
                    {"title": "备用镜头-启用", "duration_ms": 4200})

    return {
        "path": str(path),
        "total_events": seq - 1,
        "films": ["film-a", "film-b"],
        "shots": {
            "film-a": ["S001", "S002", "S003"],
            "film-b": ["S010", "S011", "S012"],
        },
        "batch": {"id": "B100", "start_seq": batch_start, "end_seq": batch_end},
        # Expected final catalog state (visible shots only).
        "expected": {
            "film-a": {
                "S001": {"title": "开场全景", "description": None,
                         "duration_ms": 13100, "scene": "SC-01", "camera": "ARRI-A",
                         "repair_note": "批量校准 13100ms"},
                "S002": {"title": "雨中特写", "description": "素材说明",
                         "duration_ms": 9100, "scene": None, "camera": None,
                         "repair_note": "批量校准 9100ms"},
                "S003": {"title": "夜景航拍", "description": "航拍组补充说明",
                         "duration_ms": 18100, "scene": None, "camera": None,
                         "repair_note": "批量校准 18100ms"},
            },
            "film-b": {
                # S010 ends withdrawn -> absent
                "S011": {"title": "对话正反打", "description": None,
                         "duration_ms": 5300, "scene": None, "camera": "SONY-V",
                         "repair_note": None},
                "S012": {"title": "备用镜头-启用", "description": None,
                         "duration_ms": 4200, "scene": None, "camera": None,
                         "repair_note": None},
            },
        },
    }


def generate_large_history(path: str | Path, n_shots: int = 100_000) -> dict[str, Any]:
    """Write the 100k-shot history: v1 creates, then one giant same-batch fix.

    Layout (all in one gapless seq stream):
      1..n_shots              v1 upserts creating shots L000001..L{n} (film-bulk)
      n+1 .. n+n_shots        v2 batch B900 upserts, same order, ms durations
      extra                   2 withdraw/republish lifecycle shots
    The batch therefore spans thousands of pages and must never be assembled
    in Python memory.
    """
    path = Path(path)
    seq = 1
    with path.open("w", encoding="utf-8") as fh:
        for i in range(1, n_shots + 1):
            shot = f"L{i:06d}"
            seq = _emit(fh, seq, "upsert", "film-bulk", shot, 1,
                        {"title": f"批量镜头 {i}", "duration_sec": 10 + (i % 7)})
        batch_start = seq
        for i in range(1, n_shots + 1):
            shot = f"L{i:06d}"
            seq = _emit(fh, seq, "upsert", "film-bulk", shot, 2,
                        {"duration_ms": (10 + (i % 7)) * 1000 + 250,
                         "repair_note": f"B900 统一毫秒化修复 #{i}"},
                        batch_id="B900")
        batch_end = seq - 1
        # withdraw/republish cases in the same film
        seq = _emit(fh, seq, "upsert", "film-bulk", "LW001", 1,
                    {"title": "撤回样本", "duration_sec": 3})
        withdraw_seq = seq
        seq = _emit(fh, seq, "withdraw", "film-bulk", "LW001", 2)
        seq = _emit(fh, seq, "upsert", "film-bulk", "LW002", 2,
                    {"title": "长期在线样本", "duration_ms": 6600})
        republish_seq = seq
        seq = _emit(fh, seq, "republish", "film-bulk", "LW001", 2)

    return {
        "path": str(path),
        "n_shots": n_shots,
        "total_events": seq - 1,
        "batch": {"id": "B900", "start_seq": batch_start, "end_seq": batch_end},
        "withdraw_seq": withdraw_seq,
        "republish_seq": republish_seq,
        "expected_visible_count": n_shots + 2,  # all L* plus LW001, LW002
        # i=1 -> 10 + (1 % 7) = 11 seconds -> v2 batch makes it 11*1000+250 ms
        "sample_shot": "L000001",
        "sample_duration_ms": (10 + (1 % 7)) * 1000 + 250,
        "sample_repair_note": "B900 统一毫秒化修复 #1",
    }


def write_tiny_history(path: str | Path) -> dict[str, Any]:
    """A handful of records for focused unit tests."""
    path = Path(path)
    seq = 1
    with path.open("w", encoding="utf-8") as fh:
        seq = _emit(fh, seq, "upsert", "f1", "a", 1,
                    {"title": "A", "duration_sec": 2, "description": "d1"})
        seq = _emit(fh, seq, "upsert", "f1", "a", 2,
                    {"duration_ms": 2500, "repair_note": "r1"})
        seq = _emit(fh, seq, "withdraw", "f1", "a", 2)
        seq = _emit(fh, seq, "republish", "f1", "a", 1)
        seq = _emit(fh, seq, "upsert", "f1", "a", 1,
                    {"description": None, "duration_sec": 3})
    return {"path": str(path), "total_events": seq - 1}


def generate_sparse_history(path: str | Path, n_unsubscribed: int = 4000,
                            batch_across: int = 600) -> dict[str, Any]:
    """History with a big unsubscribed film amid subscribed films.

    Layout (gapless seq):
      * ``film-sub-a`` / ``film-sub-b``: a few shots, revised in place;
      * ``film-skip``: ``n_unsubscribed`` ordinary revisions of one shot with
        distinct payloads -- if a sparse reader transferred them, its volume
        would grow linearly with this number;
      * a same-batch revision ``B700`` that revises a shot of ``film-sub-a``
        and a shot of ``film-skip`` back-to-back (so the batch is cross-film:
        publishing it for the subscribed film must NOT require the unsubscribed
        film's records);
      * ``film-late``: a film whose events start only *after* the subscribed
        films were already published -- the case a late (append) subscription
        backfills;
      * a trailing ordinary revision of ``film-skip`` after the batch.
    The returned summary lists exact seqs so tests can reason about windows.
    """
    path = Path(path)
    seq = 1
    with path.open("w", encoding="utf-8") as fh:
        # Early subscriber history.
        for i, (shot, ms) in enumerate((("A001", 1000), ("A002", 2000)), start=1):
            seq = _emit(fh, seq, "upsert", "film-sub-a", shot, 2,
                        {"title": f"A-{shot}", "duration_ms": ms})
        seq = _emit(fh, seq, "upsert", "film-sub-b", "B001", 1,
                    {"title": "B-1", "duration_sec": 3})
        # Bulk ordinary churn on the unsubscribed film.
        skip_start = seq
        for i in range(n_unsubscribed):
            seq = _emit(fh, seq, "upsert", "film-skip", "SK001", 2,
                        {"title": f"skip-{i}", "duration_ms": 5000 + i})
        skip_end = seq - 1
        # Cross-film same-batch revision: subscribed shot first, then skipped.
        batch_start = seq
        seq = _emit(fh, seq, "upsert", "film-sub-a", "A001", 2,
                    {"duration_ms": 1250, "repair_note": "cross-batch"},
                    batch_id="B700")
        for i in range(batch_across - 1):
            seq = _emit(fh, seq, "upsert", "film-skip", "SK001", 2,
                        {"title": f"skip-batch-{i}", "duration_ms": 7000 + i},
                        batch_id="B700")
        seq = _emit(fh, seq, "upsert", "film-sub-b", "B001", 2,
                    {"duration_ms": 3250}, batch_id="B700")
        batch_end = seq - 1
        # Trailing revision of an unsubscribed shot after the batch.
        seq = _emit(fh, seq, "upsert", "film-skip", "SK001", 2,
                    {"title": "skip-tail", "duration_ms": 9999})
        # Late film: appears only now, so an append subscription must backfill.
        late_start = seq
        for shot, ms in (("L001", 800), ("L002", 900)):
            seq = _emit(fh, seq, "upsert", "film-late", shot, 2,
                        {"title": f"late-{shot}", "duration_ms": ms})
        late_end = seq - 1
    return {
        "path": str(path),
        "total_events": seq - 1,
        "skip": {"start_seq": skip_start, "end_seq": skip_end},
        "batch": {"id": "B700", "start_seq": batch_start, "end_seq": batch_end},
        "late": {"start_seq": late_start, "end_seq": late_end},
        "subscribed": ["film-sub-a", "film-sub-b"],
        "expected": {
            "film-sub-a": {
                "A001": {"duration_ms": 1250, "repair_note": "cross-batch"},
                "A002": {"duration_ms": 2000, "repair_note": None},
            },
            "film-sub-b": {"B001": {"duration_ms": 3250}},
            "film-late": {"L001": {"duration_ms": 800},
                          "L002": {"duration_ms": 900}},
        },
    }
