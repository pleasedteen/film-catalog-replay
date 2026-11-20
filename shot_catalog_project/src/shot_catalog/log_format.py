"""Source log record format and normalization.

Log format (newline-delimited JSON, one record per line on the source side)
===========================================================================

Every record is committed to the source's append-only history and gets a
unique, gapless, monotonically increasing ``seq`` (1..N). The *meaning* of the
catalog is the source history interpreted in ``seq`` order; network delivery
may duplicate records, overlap pages and reorder everything -- ``seq`` is the
only ordering authority.

Common fields
-------------
seq            integer, gapless 1..N, the source commit order
event_id       string, globally unique idempotency key (same record may be
               redelivered any number of times with the same event_id)
event_type     "upsert" | "withdraw" | "republish"
film_id        string, e.g. "film-a"
shot_id        string, unique within a film (globally unique in fixtures)
version        workstation schema version that produced the record: 1 or 2
ts             ISO-8601 UTC timestamp of the source commit

event_type specifics
--------------------
upsert:      carries a partial/full attribute set:
             v1 (legacy workstation, duration in SECONDS):
                 title?, description?, duration_sec? (integer/float seconds),
                 scene?, camera?
             v2 (new workstation, duration in MILLISECONDS):
                 title?, description?, duration_ms? (integer milliseconds),
                 scene?, camera?, repair_note?
             Any attribute (duration included) may be omitted: an upsert is a
             *partial revision*. An attribute present as JSON null explicitly
             CLEARS that attribute (e.g. description = null empties the note
             while leaving everything else untouched). The first upsert that
             creates a shot is expected to carry a duration.
withdraw:    no attributes; the shot becomes unretrievable until a later
             republish.
republish:   no attributes; reverses the latest withdraw.

batch_id      optional string. Records that form one资料修复 (same-batch
             revision of potentially 100k shots) share one batch_id and occupy
             a contiguous run of seq values, possibly spanning many log pages.
             The synchronizer only publishes the group once its whole
             contiguous run is durably staged: all-or-nothing visibility.
             Ordinary records omit batch_id and are committed individually.

Only v2 records may carry ``repair_note``. Normalization keeps any repair note
a v2 revision set or cleared; a v1 (seconds) revision that does not mention
repair_note can never erase an existing one. Durations are always normalized
to integer milliseconds before touching DuckDB, so every query answer is in
milliseconds.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

VALID_EVENTS = ("upsert", "withdraw", "republish")
# Attributes that can appear inside an upsert payload.
UPSETTABLE = ("title", "description", "duration_ms", "scene", "camera", "repair_note")
# Attributes a v1 upsert is allowed to carry (after duration_sec conversion).
V1_ATTRS = ("title", "description", "duration_ms", "scene", "camera")
V2_ATTRS = UPSETTABLE


class LogFormatError(ValueError):
    """Raised when a source record violates the log format contract."""


@dataclass(frozen=True)
class NormalizedRecord:
    """One source record translated into the staging-table column shape.

    *_present flags say whether the attribute was present in the payload
    (including an explicit null). Snapshot construction only overwrites an
    attribute when the flag is true, so omitted attributes survive revisions
    and explicit nulls are able to clear them.
    """

    seq: int
    event_id: str
    event_type: str
    film_id: str
    shot_id: str
    version: int
    ts: str
    batch_id: str | None
    title: str | None
    title_present: bool
    description: str | None
    description_present: bool
    duration_ms: int | None
    duration_ms_present: bool
    scene: str | None
    scene_present: bool
    camera: str | None
    camera_present: bool
    repair_note: str | None
    repair_note_present: bool

    def as_staging_tuple(self) -> tuple:
        return (
            self.seq,
            self.event_id,
            self.event_type,
            self.film_id,
            self.shot_id,
            self.version,
            self.ts,
            self.batch_id,
            self.title,
            self.title_present,
            self.description,
            self.description_present,
            self.duration_ms,
            self.duration_ms_present,
            self.scene,
            self.scene_present,
            self.camera,
            self.camera_present,
            self.repair_note,
            self.repair_note_present,
        )


def _require_str(rec: Mapping[str, Any], key: str) -> str:
    val = rec.get(key)
    if not isinstance(val, str) or not val:
        raise LogFormatError(f"record seq={rec.get('seq')!r}: {key!r} must be a non-empty string")
    return val


def _require_int(rec: Mapping[str, Any], key: str) -> int:
    val = rec.get(key)
    if isinstance(val, bool) or not isinstance(val, int):
        raise LogFormatError(f"record seq={rec.get('seq')!r}: {key!r} must be an integer")
    return val


def normalize(rec: Mapping[str, Any]) -> NormalizedRecord:
    """Validate one raw JSON-decoded record and normalize it.

    Raises LogFormatError on any contract violation.
    """
    if not isinstance(rec, Mapping):
        raise LogFormatError("record must be a JSON object")
    seq = _require_int(rec, "seq")
    if seq < 1:
        raise LogFormatError(f"seq must be >= 1, got {seq}")
    event_id = _require_str(rec, "event_id")
    event_type = _require_str(rec, "event_type")
    if event_type not in VALID_EVENTS:
        raise LogFormatError(f"unknown event_type {event_type!r} at seq={seq}")
    film_id = _require_str(rec, "film_id")
    shot_id = _require_str(rec, "shot_id")
    version = _require_int(rec, "version")
    if version not in (1, 2):
        raise LogFormatError(f"unsupported workstation version {version} at seq={seq}")
    ts = _require_str(rec, "ts")
    batch_id = rec.get("batch_id")
    if batch_id is not None and (not isinstance(batch_id, str) or not batch_id):
        raise LogFormatError(f"record seq={seq}: batch_id must be a non-empty string or omitted")

    # Optional, nullable upsert attributes default to "absent".
    vals: dict[str, Any] = {name: None for name in UPSETTABLE}
    present = {name: False for name in UPSETTABLE}

    if event_type == "upsert":
        payload = rec.get("payload")
        if not isinstance(payload, Mapping):
            raise LogFormatError(f"upsert seq={seq} requires a payload object")

        # Duration is optional: an upsert may revise any subset of attributes.
        # When present, exactly one duration field matching the version is used.
        has_sec = "duration_sec" in payload
        has_ms = "duration_ms" in payload
        if has_sec and has_ms:
            raise LogFormatError(f"upsert seq={seq} carries both duration_sec and duration_ms")
        if has_ms:
            if version != 2:
                raise LogFormatError(
                    f"upsert seq={seq}: duration_ms is only valid on version 2 workstations"
                )
            dms = payload["duration_ms"]
            if isinstance(dms, bool) or not isinstance(dms, int) or dms < 0:
                raise LogFormatError(f"upsert seq={seq}: duration_ms must be a non-negative integer")
            vals["duration_ms"] = dms
            present["duration_ms"] = True
        elif has_sec:
            if version != 1:
                raise LogFormatError(
                    f"upsert seq={seq}: duration_sec is only valid on version 1 workstations"
                )
            sec = payload["duration_sec"]
            if isinstance(sec, bool) or not isinstance(sec, (int, float)) or sec < 0:
                raise LogFormatError(f"upsert seq={seq}: duration_sec must be a non-negative number")
            vals["duration_ms"] = int(round(float(sec) * 1000.0))
            present["duration_ms"] = True

        allowed = set(V2_ATTRS if version == 2 else V1_ATTRS)
        for key, val in payload.items():
            if key in ("duration_sec", "duration_ms"):
                continue
            if key not in UPSETTABLE:
                raise LogFormatError(f"upsert seq={seq}: unknown payload attribute {key!r}")
            if key not in allowed:
                raise LogFormatError(
                    f"upsert seq={seq}: attribute {key!r} is not supported by workstation v{version}"
                )
            if val is not None and not isinstance(val, str):
                raise LogFormatError(f"upsert seq={seq}: payload attribute {key!r} must be string or null")
            vals[key] = val
            present[key] = True
    else:
        payload = rec.get("payload")
        if payload:
            raise LogFormatError(f"{event_type} seq={seq} must not carry a payload")

    return NormalizedRecord(
        seq=seq,
        event_id=event_id,
        event_type=event_type,
        film_id=film_id,
        shot_id=shot_id,
        version=version,
        ts=ts,
        batch_id=batch_id,
        title=vals["title"],
        title_present=present["title"],
        description=vals["description"],
        description_present=present["description"],
        duration_ms=vals["duration_ms"],
        duration_ms_present=present["duration_ms"],
        scene=vals["scene"],
        scene_present=present["scene"],
        camera=vals["camera"],
        camera_present=present["camera"],
        repair_note=vals["repair_note"],
        repair_note_present=present["repair_note"],
    )


def normalize_line(line: str) -> NormalizedRecord:
    """Parse and normalize one JSON line."""
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as exc:
        raise LogFormatError(f"invalid JSON line: {exc}") from exc
    return normalize(raw)


def normalize_staged_line(line: str) -> NormalizedRecord:
    """Parse a normalized staging-shape record (see Catalog.export_history_jsonl).

    These lines are produced from already-validated staging rows and always
    carry normalized millisecond durations plus explicit ``*_present`` flags,
    so they are reconstructed directly without re-applying the v1/v2 payload
    rules (a normalized v1 row legitimately stores a duration_ms value). This
    is the offline rebuild path used after the HTTP log source is gone.
    """
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as exc:
        raise LogFormatError(f"invalid JSON line: {exc}") from exc
    if not isinstance(raw, Mapping) or "event_type" not in raw:
        raise LogFormatError("not a normalized staging record")
    payload = raw.get("payload") or {}

    def present(attr: str) -> bool:
        return attr in payload

    vals = {name: payload.get(name) for name in UPSETTABLE}
    return NormalizedRecord(
        seq=_require_int(raw, "seq"),
        event_id=_require_str(raw, "event_id"),
        event_type=_require_str(raw, "event_type"),
        film_id=_require_str(raw, "film_id"),
        shot_id=_require_str(raw, "shot_id"),
        version=_require_int(raw, "version"),
        ts=_require_str(raw, "ts"),
        batch_id=raw.get("batch_id"),
        title=vals["title"], title_present=present("title"),
        description=vals["description"],
        description_present=present("description"),
        duration_ms=vals["duration_ms"],
        duration_ms_present=present("duration_ms"),
        scene=vals["scene"], scene_present=present("scene"),
        camera=vals["camera"], camera_present=present("camera"),
        repair_note=vals["repair_note"],
        repair_note_present=present("repair_note"),
    )


def record_to_json(rec: Mapping[str, Any]) -> str:
    """Serialize a raw record dict back to a log line."""
    return json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
