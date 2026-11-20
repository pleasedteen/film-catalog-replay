"""Independently maintainable cold storage for already-published commit history.

The hot DuckDB database keeps only the working set: freshly staged events and
the published prefix that has not been archived yet. Everything that has
already been published (``seq <= sync_state.last_snapshot_seq``) can be moved
to the cold side as immutable, independently maintained Parquet segment files
plus a JSON manifest. The hot database directory layout and the published
water mark never change because of archival -- only rows below the water mark
move, always in one crash-safe sequence.

Crash safety contract of one archive segment
=============================================
The state table ``cold_archive_state`` inside the hot database is the single
source of truth about what is archived. A segment goes through these steps:

  1. COPY the hot rows to ``segments/<name>.parquet.tmp`` and read them back
     to verify row count, seq range and a content fingerprint;
  2. rename the tmp file to its final ``*.parquet`` name (a crash here leaves
     an *orphan* file that no state row references -- deleted on reopen);
  3. one DuckDB transaction deletes the hot rows and inserts the state row,
     then CHECKPOINT (a crash here is impossible to partially observe: either
     the old hot-only state or the new archived state survives);
  4. rewrite ``manifest.json`` atomically from the state table (a crash here
     is repaired on reopen by regenerating the manifest from state).

On reopen, :meth:`ColdStorage.reconcile` removes tmp/orphan files, regenerates
a missing or stale manifest, and flips the cold side offline if a referenced
segment file is gone. All cold-touching operations raise
:class:`ColdUnavailable` while offline; the hot query path (``shot_state``)
never reads the cold side and keeps serving.

The cold root is a plain directory: it can be backed up, moved or inspected
with any Parquet tool without touching the hot database.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

log = logging.getLogger("shot_catalog.cold")

MANIFEST_NAME = "manifest.json"
SEGMENTS_DIR = "segments"
SEGMENT_SUFFIX = ".parquet"
TMP_SUFFIX = ".parquet.tmp"


class ColdUnavailable(RuntimeError):
    """Raised when a cold-side operation cannot be served right now.

    Hot-only operations (staging, publishing, shot queries) are unaffected and
    must keep working; callers treat this as a backpressure signal, not a fatal
    error.
    """


@dataclass(frozen=True)
class SegmentInfo:
    seg_id: int
    file_name: str
    start_seq: int
    end_seq: int
    row_count: int
    checksum: str
    archived_at: str

    @classmethod
    def from_row(cls, row: tuple) -> "SegmentInfo":
        return cls(
            seg_id=int(row[0]), start_seq=int(row[1]), end_seq=int(row[2]),
            row_count=int(row[3]), checksum=str(row[4]), file_name=str(row[5]),
            archived_at=str(row[6]),
        )

    def to_manifest(self) -> dict:
        return {
            "seg_id": self.seg_id, "file": self.file_name,
            "start_seq": self.start_seq, "end_seq": self.end_seq,
            "row_count": self.row_count, "checksum": self.checksum,
            "archived_at": self.archived_at,
        }


class ColdStorage:
    """File-level cold side: segment paths, manifest, availability flags.

    Parameters
    ----------
    root:
        Cold directory. Created on demand; everything lives below it so the
        cold side is one self-contained, independently maintainable folder.
    failure_hook:
        Optional test hook called with an operation name before every cold
        file operation; raising ColdUnavailable there simulates a cold fault.
    """

    def __init__(self, root: str | Path, *,
                 failure_hook: Callable[[str], None] | None = None):
        self.root = Path(root)
        self.segments_dir = self.root / SEGMENTS_DIR
        self.manifest_path = self.root / MANIFEST_NAME
        self._online = True
        self.failure_hook = failure_hook

    # ------------------------------------------------------------ availability
    @property
    def available(self) -> bool:
        return self._online

    def set_offline(self) -> None:
        """Simulate a cold-side outage (missing mount, permission loss, ...)."""
        if self._online:
            log.warning("cold storage switched OFFLINE at %s", self.root)
        self._online = False

    def set_online(self) -> None:
        if not self._online:
            log.warning("cold storage back ONLINE at %s", self.root)
        self._online = True

    def check_available(self, operation: str) -> None:
        if not self._online:
            raise ColdUnavailable(f"cold storage offline during {operation}")
        if self.failure_hook is not None:
            self.failure_hook(operation)

    def ensure_dirs(self) -> None:
        self.check_available("ensure_dirs")
        self.segments_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------- paths
    def segment_name(self, seg_id: int) -> str:
        return f"seg-{seg_id:08d}{SEGMENT_SUFFIX}"

    def segment_path(self, seg_id: int) -> Path:
        return self.segments_dir / self.segment_name(seg_id)

    def tmp_segment_path(self, seg_id: int) -> Path:
        return self.segments_dir / f"seg-{seg_id:08d}{TMP_SUFFIX}"

    def list_segment_files(self) -> list[Path]:
        """Final segment files currently on disk (glob does not match tmp)."""
        if not self.segments_dir.exists():
            return []
        return sorted(self.segments_dir.glob(f"*{SEGMENT_SUFFIX}"))

    def list_tmp_files(self) -> list[Path]:
        if not self.segments_dir.exists():
            return []
        return sorted(self.segments_dir.glob(f"*{TMP_SUFFIX}"))

    def rename_tmp_to_final(self, seg_id: int) -> None:
        self.check_available("segment_rename")
        os.replace(self.tmp_segment_path(seg_id), self.segment_path(seg_id))

    def remove_file(self, path: Path) -> None:
        self.check_available("remove_orphan")
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    # --------------------------------------------------------------- manifest
    def read_manifest(self) -> dict | None:
        self.check_available("read_manifest")
        if not self.manifest_path.exists():
            return None
        with self.manifest_path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def write_manifest(self, segments: list[SegmentInfo],
                       archived_through_seq: int) -> None:
        """Atomically replace the manifest (tmp file + os.replace)."""
        self.check_available("write_manifest")
        self.root.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "archived_through_seq": int(archived_through_seq),
            "segment_count": len(segments),
            "segments": [s.to_manifest() for s in segments],
        }
        tmp = self.manifest_path.with_name(MANIFEST_NAME + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.manifest_path)

    # ------------------------------------------------------------- reconciliation
    def reconcile(self, committed: list[SegmentInfo]) -> dict:
        """Repair the cold folder against the durable state table.

        The state table is authoritative. Returns a report dict; flips the
        storage offline if a committed segment is missing on disk.
        """
        report = {"tmp_removed": 0, "orphans_removed": 0,
                  "manifest_regenerated": False, "missing_segments": []}
        if not self._online:
            return report
        self.ensure_dirs()

        # 1) tmp files can never be referenced by state -> always safe to drop.
        for tmp in self.list_tmp_files():
            self.remove_file(tmp)
            report["tmp_removed"] += 1

        committed_by_name = {s.file_name: s for s in committed}
        on_disk = {p.name: p for p in self.list_segment_files()}

        # 2) files with no state row are leftovers of a crash between the
        #    rename and the state transaction -> delete them.
        for name, path in on_disk.items():
            if name not in committed_by_name:
                self.remove_file(path)
                report["orphans_removed"] += 1

        # 3) state rows whose file vanished -> the cold side is unreadable.
        for info in committed:
            if not (self.segments_dir / info.file_name).exists():
                report["missing_segments"].append(info.file_name)
        if report["missing_segments"]:
            self.set_offline()
            return report

        # 4) the manifest is a derived, regenerable index. Rewrite it whenever
        #    it is absent or does not match the authoritative state.
        through = max((s.end_seq for s in committed), default=0)
        manifest = self.read_manifest()
        expected = [s.to_manifest() for s in committed]
        if manifest is None or manifest.get("segments") != expected \
                or manifest.get("archived_through_seq") != through:
            self.write_manifest(committed, through)
            report["manifest_regenerated"] = True
        return report
