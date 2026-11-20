"""DuckDB-backed durable shot catalog with hot/cold history separation.

Storage model
=============
* ``event_staging`` (HOT) -- every normalized source record not yet archived,
  keyed by source seq. Staging is an append-only, idempotent path: in-page
  duplicates are folded in Python (only the current page is buffered) and
  cross-page redelivery is discarded by a primary-key anti-join against the
  HOT table only, so Python never holds more than one fetched page at a time
  and never assembles a whole batch.

* cold segment files (COLD) -- already-published history that a background
  archival step moved out of the hot database into independently maintainable
  Parquet segments (see ``cold.py``). A ``cold_archive_state`` table inside
  the hot database is the authoritative index of what moved; the on-disk
  manifest and Parquet files are derived/repair-able from it.

* ``event_history`` -- a view over hot staging UNION cold segments, with hot
  rows winning on seq overlap (redelivery of an archived page lands back in
  hot staging and shadows its cold copy). It is the table used by full-history
  rebuild/verification; the per-commit delta path only ever touches hot rows.

* ``shot_state`` -- one row per shot ever seen, holding its current
  attributes and a ``withdrawn`` flag. ``catalog_current`` is a VIEW of the
  visible rows (``NOT withdrawn``), so a withdrawn shot simply disappears from
  every query and reappears on republish. Queries never read the cold side,
  so a cold outage cannot block them.

* ``sync_state`` -- the single durable progress pointer
  (``last_snapshot_seq``), updated in the SAME transaction as the state it
  describes. Saved progress and query answers therefore always refer to the
  same complete prefix of source history. Archival never moves this pointer.

Incremental commits
===================
Applying a commit group (one ordinary record, a coalesced run of ordinary
records, or one 100k-shot same-batch revision) is a single transaction that
builds a per-shot delta from ONLY the group's seq range inside DuckDB (a
temp table + UPDATE + INSERT), then advances the progress pointer. Cost is
proportional to group size, never to history size, and no batch is ever
assembled in Python: the only arguments are two seq integers. Partial
revisions are honored via per-attribute ``*_changed`` flags; explicit nulls
flip the flag while carrying a null value, so they can clear an attribute.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .cold import ColdStorage, ColdUnavailable, SegmentInfo
from .log_format import NormalizedRecord

STAGING_COLUMNS = """
    seq BIGINT PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    film_id VARCHAR NOT NULL,
    shot_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    ts VARCHAR NOT NULL,
    batch_id VARCHAR,
    title VARCHAR,
    title_present BOOLEAN NOT NULL,
    description VARCHAR,
    description_present BOOLEAN NOT NULL,
    duration_ms BIGINT,
    duration_ms_present BOOLEAN NOT NULL,
    scene VARCHAR,
    scene_present BOOLEAN NOT NULL,
    camera VARCHAR,
    camera_present BOOLEAN NOT NULL,
    repair_note VARCHAR,
    repair_note_present BOOLEAN NOT NULL
"""
# Column order used for bulk appends and page-level dedupe.
STAGING_COLS = [c.strip().split()[0] for c in STAGING_COLUMNS.strip().rstrip(",").split(",")]

STATE_COLUMNS = """
    film_id VARCHAR NOT NULL,
    shot_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    last_event_seq BIGINT NOT NULL,
    last_ts VARCHAR NOT NULL,
    title VARCHAR,
    description VARCHAR,
    duration_ms BIGINT,
    scene VARCHAR,
    camera VARCHAR,
    repair_note VARCHAR,
    withdrawn BOOLEAN NOT NULL,
    PRIMARY KEY (film_id, shot_id)
"""

_ATTRS = ("title", "description", "duration_ms", "scene", "camera", "repair_note")

# Per-shot delta for one seq range, evaluated entirely inside DuckDB. A single
# GROUP BY finds each attribute's last present-event seq (and the latest
# withdraw/republish state); the self joins read those events' values.
# Reading values via the seq join -- rather than arg_max(value, seq) -- is
# required because DuckDB's arg_max forgets a value after seeing null, while
# an explicit JSON null must be able to clear an attribute. *_changed is false
# when the range never presented the attribute, so the UPDATE keeps the
# previous value (partial revisions never clobber untouched fields).
_DELTA_GROUP_SQL = """
CREATE TEMP TABLE _delta AS
WITH g AS (
    SELECT
        film_id,
        shot_id,
        arg_max(version, seq) AS dv,
        max(seq) AS dlast,
        arg_max(ts, seq) AS dts,
        {latest_seqs},
        arg_max(CASE WHEN event_type = 'withdraw' THEN 1
                     WHEN event_type = 'republish' THEN 0 END, seq) AS dwd
    FROM __EVENT_TABLE__
    WHERE seq BETWEEN $start_seq AND $end_seq
    GROUP BY film_id, shot_id
)
SELECT
    g.film_id, g.shot_id, g.dv, g.dlast, g.dts, g.dwd,
    {join_selects}
FROM g
{joins}
"""

_LATEST_SEQS = ",\n        ".join(
    f"max(seq) FILTER (WHERE {a}_present) AS {a}_seq" for a in _ATTRS
)
_JOIN_CLAUSES = "\n".join(
    f"LEFT JOIN __EVENT_TABLE__ {a}e "
    f"ON {a}e.seq = g.{a}_seq AND {a}e.film_id = g.film_id AND {a}e.shot_id = g.shot_id"
    for a in _ATTRS
)
_JOIN_SELECTS = ",\n    ".join(
    f"{a}e.{a} AS d_{a}, (g.{a}_seq IS NOT NULL) AS {a}_changed" for a in _ATTRS
)

# Full prefix reconstruction, used only by tests as an independent oracle to
# verify the incremental state equals interpreting the whole history at once.
# Full-prefix reconstruction used only by tests as an independent oracle.
# It uses the same GROUP BY + value-join idea as the delta but recomputes
# every shot from the whole staging prefix in one statement, whereas
# production state is folded incrementally per commit group.
_REBUILD_SQL = """
CREATE OR REPLACE TEMP TABLE _rebuild AS
WITH g AS (
    SELECT
        film_id, shot_id,
        arg_max(version, seq) AS version,
        max(seq) AS last_event_seq,
        arg_max(ts, seq) AS last_ts,
        {latest_seqs},
        COALESCE(arg_max(CASE WHEN event_type = 'withdraw' THEN 1
                              WHEN event_type = 'republish' THEN 0 END, seq), false) AS withdrawn
    FROM __EVENT_TABLE__
    WHERE seq <= $last_seq
    GROUP BY film_id, shot_id
)
SELECT
    g.film_id, g.shot_id, g.version, g.last_event_seq, g.last_ts,
    {selects}, g.withdrawn
FROM g
{joins}
"""

_REBUILD_LATEST_SEQS = ",\n    ".join(
    f"max(seq) FILTER (WHERE {a}_present) AS {a}_seq" for a in _ATTRS
)
_REBUILD_SELECTS = ",\n    ".join(f"{a}e.{a} AS {a}" for a in _ATTRS)


@dataclass(frozen=True)
class CommitGroup:
    """A contiguous run of seqs committed atomically."""

    start_seq: int
    end_seq: int
    batch_id: str | None  # None => ordinary single-record group

    @property
    def size(self) -> int:
        return self.end_seq - self.start_seq + 1


@dataclass(frozen=True)
class ArchiveResult:
    """One cold segment produced by an archival step."""

    seg_id: int
    start_seq: int
    end_seq: int
    row_count: int
    checksum: str
    file_name: str

    @property
    def size(self) -> int:
        return self.end_seq - self.start_seq + 1


# Column list of event_staging / cold segment Parquet in SELECT order.
_HISTORY_COLS = ", ".join(STAGING_COLS)


class Catalog:
    def __init__(self, db_path: str | Path,
                 cold: ColdStorage | str | Path | None = None):
        self.db_path = str(db_path)
        # One connection guarded by an RLock: DuckDB disallows mixing
        # read-only and read-write connections to one file in a process.
        # Reads are short autocommit statements, so a film listing observes
        # one coherent MVCC snapshot while the sync thread waits its turn.
        self._lock = threading.RLock()
        self._conn = duckdb.connect(self.db_path)
        self._stage_ready = False
        if isinstance(cold, ColdStorage) or cold is None:
            self.cold = cold
        else:
            self.cold = ColdStorage(cold)
        self._cold_view_ready = False
        self._init_schema()
        if self.cold is not None:
            self._reconcile_cold()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        conn = self._conn
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(f"CREATE TABLE IF NOT EXISTS event_staging ({STAGING_COLUMNS})")
            conn.execute(f"CREATE TABLE IF NOT EXISTS shot_state ({STATE_COLUMNS})")
            conn.execute("""
                CREATE VIEW IF NOT EXISTS catalog_current AS
                SELECT film_id, shot_id, version, last_event_seq, last_ts,
                       title, description, duration_ms, scene, camera, repair_note
                FROM shot_state
                WHERE NOT withdrawn
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    last_snapshot_seq BIGINT NOT NULL,
                    updated_at VARCHAR NOT NULL,
                    CHECK (id = 1)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS catalog_meta (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cold_archive_state (
                    seg_id INTEGER PRIMARY KEY,
                    start_seq BIGINT NOT NULL,
                    end_seq BIGINT NOT NULL,
                    row_count BIGINT NOT NULL,
                    checksum VARCHAR NOT NULL,
                    file_name VARCHAR NOT NULL,
                    archived_at VARCHAR NOT NULL
                )
            """)
            conn.execute("""
                INSERT INTO sync_state (id, last_snapshot_seq, updated_at)
                SELECT 1, 0, strftime(now(), '%Y-%m-%dT%H:%M:%SZ')
                WHERE NOT EXISTS (SELECT 1 FROM sync_state)
            """)
            # seq already has a PRIMARY KEY index; an extra batch_id index
            # only slows bulk staging -- batch runs are contiguous in seq and
            # are found via range scans on the primary key.
            conn.execute("CREATE INDEX IF NOT EXISTS idx_shot_film ON shot_state(film_id)")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "Catalog":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -------------------------------------------------------------- cold side
    def cold_available(self) -> bool:
        """True when no cold side is configured or the cold side is healthy."""
        return self.cold is None or self.cold.available

    def _committed_segments(self) -> list[SegmentInfo]:
        rows = self._conn.execute("""
            SELECT seg_id, start_seq, end_seq, row_count, checksum,
                   file_name, archived_at
            FROM cold_archive_state ORDER BY seg_id
        """).fetchall()
        return [SegmentInfo.from_row(r) for r in rows]

    def _reconcile_cold(self) -> dict:
        """Repair cold files/manifest against the authoritative state table."""
        assert self.cold is not None
        with self._lock:
            segments = self._committed_segments()
            report = self.cold.reconcile(segments)
            # A missing committed segment flips the cold side offline; the
            # catalog must still open, so only build the history view when
            # the cold side is readable.
            if self.cold.available:
                self._refresh_cold_view()
            else:
                self._cold_view_ready = False
            return report

    def _refresh_cold_view(self) -> None:
        """(Re)create the hot+cold history view from the Parquet segment glob.

        Only created when at least one segment exists, so a database with no
        archives never depends on a Parquet glob resolving. Raises
        ColdUnavailable when the cold side is offline: callers of archive /
        verification propagate it, while the hot query path never gets here.
        """
        if self.cold is None:
            self._cold_view_ready = False
            return
        self.cold.check_available("refresh_cold_view")
        conn = self._conn
        conn.execute("DROP VIEW IF EXISTS event_history")
        files = self.cold.list_segment_files()
        if not files:
            self._cold_view_ready = False
            return
        glob = str(self.cold.segments_dir / "*.parquet")
        # Hot rows shadow cold rows while present; redelivered archived rows
        # remain authoritative in the hot table until their content is verified
        # against the Parquet copies and reclaimed.
        conn.execute(f"""
            CREATE VIEW event_history AS
            SELECT {_HISTORY_COLS} FROM event_staging
            UNION ALL
            SELECT {_HISTORY_COLS} FROM read_parquet('{glob}') h
            WHERE NOT EXISTS (
                SELECT 1 FROM event_staging e WHERE e.seq = h.seq
            )
        """)
        self._cold_view_ready = True

    def _history_table(self) -> str:
        """Table/view covering the whole durable history (hot + cold)."""
        if self.cold is not None and self._cold_view_ready:
            self.cold.check_available("read_cold_history")
            return "event_history"
        return "event_staging"

    def archived_through_seq(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT max(end_seq) FROM cold_archive_state"
            ).fetchone()
        return int(row[0] or 0)

    def _set_meta(self, key: str, value: str) -> None:
        self._conn.execute("""
            INSERT INTO catalog_meta VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, [key, value])

    def _get_meta(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM catalog_meta WHERE key = ?", [key]
        ).fetchone()
        return None if row is None else row[0]

    def record_fetch_watermark(self, seq: int) -> None:
        """Durable high-water mark of the highest seq ever pulled."""
        with self._lock:
            if seq > self.fetch_watermark():
                self._set_meta("max_fetch_seq", str(int(seq)))

    def fetch_watermark(self) -> int:
        with self._lock:
            raw = self._get_meta("max_fetch_seq")
        return int(raw or 0)

    def hot_published_group_count(self) -> int:
        """Published-but-not-archived commit groups still held in the hot db.

        Ordinary (batch_id IS NULL) contiguous rows count as one coalesced
        group; every same-batch run counts as one group. This is the backlog
        the sync engine bounds while the cold side is unavailable.
        """
        with self._lock:
            archived = self.archived_through_seq()
            ordinary_groups = int(self._conn.execute("""
                SELECT count(*) FROM (
                    SELECT grp FROM (
                        SELECT seq,
                               seq - row_number() OVER (ORDER BY seq) AS grp
                        FROM event_staging
                        WHERE seq <= (SELECT last_snapshot_seq FROM sync_state WHERE id = 1)
                          AND seq > ? AND batch_id IS NULL
                    )
                    GROUP BY grp
                )
            """, [archived]).fetchone()[0])
            batch_groups = int(self._conn.execute("""
                SELECT count(DISTINCT batch_id) FROM event_staging
                WHERE seq <= (SELECT last_snapshot_seq FROM sync_state WHERE id = 1)
                  AND seq > ? AND batch_id IS NOT NULL
            """, [archived]).fetchone()[0])
            return ordinary_groups + batch_groups

    def _reclaim_shadowed_hot_rows(self) -> int:
        """Drop hot rows that merely redeliver an already-archived seq.

        Old-page redelivery lands back in hot staging; the hot+cold view lets
        those rows shadow their cold copy. They can never be re-archived (their
        seq is below the archival boundary), so once their content is verified
        identical to the cold segment they came from, they are deleted here to
        shrink the hot database back to its working set. Returns the row count.
        """
        if self.cold is None or not self._cold_view_ready:
            return 0
        with self._lock:
            # Never reclaim while the cold side is unreadable: shadow rows are
            # then the only local copy of their seqs.
            self.cold.check_available("reclaim_shadowed_rows")
            archived = self.archived_through_seq()
            if archived <= 0:
                return 0
            glob = str(self.cold.segments_dir / "*.parquet")
            # Compare against the raw Parquet segments (not the hot+cold view,
            # where the redelivered hot row would shadow its cold copy).
            mismatch = self._conn.execute(f"""
                SELECT count(*) FROM event_staging e
                JOIN read_parquet('{glob}') c ON c.seq = e.seq
                WHERE e.seq <= {archived}
                  AND (e.event_id IS DISTINCT FROM c.event_id
                       OR e.event_type IS DISTINCT FROM c.event_type
                       OR e.film_id IS DISTINCT FROM c.film_id
                       OR e.shot_id IS DISTINCT FROM c.shot_id
                       OR e.version IS DISTINCT FROM c.version
                       OR e.ts IS DISTINCT FROM c.ts
                       OR e.batch_id IS DISTINCT FROM c.batch_id
                       OR e.title IS DISTINCT FROM c.title
                       OR e.title_present IS DISTINCT FROM c.title_present
                       OR e.description IS DISTINCT FROM c.description
                       OR e.description_present IS DISTINCT FROM c.description_present
                       OR e.duration_ms IS DISTINCT FROM c.duration_ms
                       OR e.duration_ms_present IS DISTINCT FROM c.duration_ms_present
                       OR e.scene IS DISTINCT FROM c.scene
                       OR e.scene_present IS DISTINCT FROM c.scene_present
                       OR e.camera IS DISTINCT FROM c.camera
                       OR e.camera_present IS DISTINCT FROM c.camera_present
                       OR e.repair_note IS DISTINCT FROM c.repair_note
                       OR e.repair_note_present IS DISTINCT FROM c.repair_note_present)
            """).fetchone()[0]
            if int(mismatch) != 0:
                raise RuntimeError(
                    "redelivered hot row differs from its archived cold copy"
                )
            count = int(self._conn.execute("""
                SELECT count(*) FROM event_staging WHERE seq <= ?
            """, [archived]).fetchone()[0])
            if count:
                self._conn.execute(
                    "DELETE FROM event_staging WHERE seq <= ?", [archived])
            return count

    def archive_published_prefix(self, target_rows: int = 5000) -> ArchiveResult | None:
        """Move one bounded slice of already-published hot history to the cold
        side without touching the directory layout or the published watermark.

        Only rows with ``archived_through < seq <= last_snapshot_seq`` are
        eligible, so unpublished (staged-ahead) data can never be archived.
        The slice is cut at batch boundaries: a same-batch group is never
        split across segments. Returns None when nothing is archivable.
        """
        if self.cold is None:
            raise ColdUnavailable("no cold storage configured")
        with self._lock:
            conn = self._conn
            self._reclaim_shadowed_hot_rows()
            snapshot = self.last_snapshot_seq()
            archived = self.archived_through_seq()
            start = archived + 1
            if start > snapshot:
                return None
            present = conn.execute(
                "SELECT count(*) FROM event_staging WHERE seq = ?", [start]
            ).fetchone()[0]
            if not present:
                return None  # nothing hot to archive at the prefix
            self.cold.ensure_dirs()
            next_seg = int(conn.execute(
                "SELECT coalesce(max(seg_id), 0) + 1 FROM cold_archive_state"
            ).fetchone()[0])

            # Upper bound of this slice, then pull back to a batch boundary.
            want_end = min(snapshot, start + target_rows - 1)
            row = conn.execute(
                "SELECT batch_id FROM event_staging WHERE seq = ?", [start]
            ).fetchone()
            if row[0] is not None:
                # Whole same-batch run, wherever it ends within the prefix.
                end_row = conn.execute("""
                    SELECT max(seq) FROM event_staging
                    WHERE seq BETWEEN ? AND ? AND batch_id = ?
                """, [start, snapshot, row[0]]).fetchone()
                end_seq = int(end_row[0])
            else:
                boundary = conn.execute("""
                    SELECT min(seq) FROM event_staging
                    WHERE seq BETWEEN ? AND ? AND batch_id IS NOT NULL
                """, [start, want_end]).fetchone()
                if boundary[0] is not None:
                    end_seq = int(boundary[0]) - 1
                else:
                    end_row = conn.execute("""
                        SELECT max(seq) FROM event_staging
                        WHERE seq BETWEEN ? AND ? AND batch_id IS NULL
                    """, [start, want_end]).fetchone()
                    end_seq = int(end_row[0])

            tmp_path = self.cold.tmp_segment_path(next_seg)
            final_name = self.cold.segment_name(next_seg)
            # 1) export + read-back verification BEFORE anything is deleted.
            conn.execute(f"""
                COPY (
                    SELECT {_HISTORY_COLS} FROM event_staging
                    WHERE seq BETWEEN {start} AND {end_seq} ORDER BY seq
                ) TO '{tmp_path}' (FORMAT PARQUET)
            """)
            self.cold.rename_tmp_to_final(next_seg)
            verify = conn.execute(f"""
                SELECT count(*), min(seq), max(seq),
                       coalesce(sum(hash(seq::VARCHAR || '|' || event_id) % 1000000007), 0)
                FROM read_parquet('{self.cold.segment_path(next_seg)}')
            """).fetchone()
            row_count, min_seq, max_seq, checksum = verify
            expected = conn.execute("""
                SELECT count(*), min(seq), max(seq),
                       coalesce(sum(hash(seq::VARCHAR || '|' || event_id) % 1000000007), 0)
                FROM event_staging WHERE seq BETWEEN ? AND ?
            """, [start, end_seq]).fetchone()
            if (int(row_count), int(min_seq), int(max_seq), str(checksum)) != (
                int(expected[0]), int(expected[1]), int(expected[2]), str(expected[3])
            ):
                self.cold.remove_file(self.cold.segment_path(next_seg))
                raise RuntimeError(
                    f"cold segment {next_seg} read-back verification failed"
                )

            # 2) the destructive half is one transaction + checkpoint.
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    "DELETE FROM event_staging WHERE seq BETWEEN ? AND ?",
                    [start, end_seq],
                )
                conn.execute("""
                    INSERT INTO cold_archive_state
                        (seg_id, start_seq, end_seq, row_count, checksum,
                         file_name, archived_at)
                    VALUES (?, ?, ?, ?, ?, ?, strftime(now(), '%Y-%m-%dT%H:%M:%SZ'))
                """, [next_seg, start, end_seq, row_count, str(checksum),
                      final_name])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("CHECKPOINT")

            # 3) derived manifest + view; both regenerable from state on reopen.
            segments = self._committed_segments()
            self.cold.write_manifest(segments, end_seq)
            self._refresh_cold_view()
            return ArchiveResult(
                seg_id=next_seg, start_seq=start, end_seq=end_seq,
                row_count=int(row_count), checksum=str(checksum),
                file_name=final_name,
            )

    def drain_hot_to_cold(self, target_rows: int = 5000) -> list[ArchiveResult]:
        """Archive every currently eligible slice; used by the one-click
        verification and by the automatic catch-up after a cold outage."""
        results: list[ArchiveResult] = []
        while True:
            piece = self.archive_published_prefix(target_rows)
            if piece is None:
                return results
            results.append(piece)

    # ------------------------------------------------- local-history rebuild
    def export_history_jsonl(self, jsonl_path: str | Path) -> int:
        """Write the full durable history (cold + hot) as gapless JSONL.

        The staging rows store normalized columns rather than the original raw
        payload shape, so this emits a normalized JSONL that ``normalize`` and
        ``load_jsonl_history`` can round-trip. It is the offline source used to
        rebuild and verify the catalog after the HTTP log source is gone.
        """
        from .log_format import record_to_json
        path = Path(jsonl_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = self._history_table()
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_HISTORY_COLS} FROM {table} ORDER BY seq"
            ).fetchall()
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                rec = _staging_row_to_raw(dict(zip(STAGING_COLS, row)))
                fh.write(record_to_json(rec) + "\n")
        return len(rows)

    def load_jsonl_history(self, jsonl_path: str | Path) -> int:
        """Load normalized JSONL history into hot staging (idempotent).

        This rebuild path does not need the HTTP log source at all: the local
        history file is enough to repopulate staging for a full verification.
        """
        from .log_format import normalize_staged_line
        path = Path(jsonl_path)
        count = 0
        batch: list[NormalizedRecord] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    batch.append(normalize_staged_line(line))
                    if len(batch) >= 1000:
                        count += self.stage_records(batch)
                        batch = []
        if batch:
            count += self.stage_records(batch)
        return count

    def verify_full_history_oracle(self) -> int:
        """Rebuild the catalog from the whole hot+cold history and compare.

        The rebuild is bounded by the published snapshot, not by the max
        staged seq: rows staged ahead of the watermark are, by design, not yet
        reflected in ``shot_state``, so including them would manufacture
        mismatches. With archives present the history view spans cold + hot,
        proving the already-archived prefix still reconstructs exactly the
        current state. Raises ColdUnavailable if archived segments exist but
        the cold side is currently unreadable.
        """
        with self._lock:
            if self.cold is not None and self.archived_through_seq() > 0:
                self.cold.check_available("verify_full_history_oracle")
                if not self._cold_view_ready:
                    raise ColdUnavailable(
                        "cold history view unavailable during verification")
            table = self._history_table()
            last_seq = self.last_snapshot_seq()
            self._conn.execute("DROP TABLE IF EXISTS _rebuild")
            self._conn.execute(
                _REBUILD_SQL.format(latest_seqs=_REBUILD_LATEST_SEQS,
                                    selects=_REBUILD_SELECTS,
                                    joins=_JOIN_CLAUSES,
                                    ).replace("__EVENT_TABLE__", table),
                {"last_seq": last_seq},
            )
            mismatches = self._conn.execute("""
                SELECT count(*) FROM (
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM shot_state
                    EXCEPT
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM _rebuild
                    UNION ALL
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM _rebuild
                    EXCEPT
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM shot_state
                )
            """).fetchone()[0]
            self._conn.execute("DROP TABLE IF EXISTS _rebuild")
            return int(mismatches)

    # ------------------------------------------------------------------ staging
    def stage_records(self, records: Iterable[NormalizedRecord]) -> int:
        """Durably append the page's not-yet-staged records. Returns new count.

        Redelivery is folded in two cheap layers: within a page, duplicate
        seq values are collapsed in Python (the page is the only buffered
        collection, bounded by page size); across pages an indexed anti-join
        keeps only seqs absent from staging. There is no per-row conflict
        merge and no repeated temp-table churn against the growing table, so
        staging stays a pure high-throughput append path. Records are
        immutable per seq, so an already-staged seq never needs overwriting.
        """
        by_seq: dict[int, tuple] = {}
        for rec in records:
            by_seq[rec.seq] = rec.as_staging_tuple()
        if not by_seq:
            return 0
        rows = [by_seq[s] for s in sorted(by_seq)]
        with self._lock:
            conn = self._conn
            if not self._stage_ready:
                # One session-scoped staging buffer, reused across pages.
                conn.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS _stage_in AS "
                    "SELECT * FROM event_staging LIMIT 0"
                )
                self._stage_ready = True
            conn.execute("TRUNCATE TABLE _stage_in")
            with conn.appender("_stage_in") as appender:
                for row in rows:
                    appender.append_row(*row)
            # Count, then append only seqs that staging does not have yet.
            inserted = conn.execute("""
                SELECT count(*) FROM _stage_in s
                WHERE NOT EXISTS (
                    SELECT 1 FROM event_staging e WHERE e.seq = s.seq
                )
            """).fetchone()[0]
            conn.execute("""
                INSERT INTO event_staging
                SELECT s.* FROM _stage_in s
                WHERE NOT EXISTS (
                    SELECT 1 FROM event_staging e WHERE e.seq = s.seq
                )
            """)
        return int(inserted)

    def max_staged_seq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT max(seq) FROM event_staging").fetchone()
        return int(row[0] or 0)

    def last_snapshot_seq(self) -> int:
        with self._lock:
            return int(
                self._conn.execute(
                    "SELECT last_snapshot_seq FROM sync_state WHERE id = 1"
                ).fetchone()[0]
            )

    # ------------------------------------------------------------- group logic
    def next_committable_group(self) -> CommitGroup | None:
        """Return the next prefix group whose whole run is durably staged.

        Ordinary records form single-seq groups; records sharing a batch_id
        form one group spanning their full contiguous run. A group is
        committable only when the whole run is staged and attaches exactly to
        the published prefix. A half-staged batch blocks everything after it.
        """
        with self._lock:
            start = self.last_snapshot_seq() + 1
            present = self._conn.execute(
                "SELECT count(*) FROM event_staging WHERE seq = ?", [start]
            ).fetchone()[0]
            if not present:
                return None
            batch_id = self._conn.execute(
                "SELECT batch_id FROM event_staging WHERE seq = ?", [start]
            ).fetchone()[0]
            if batch_id is None:
                return CommitGroup(start, start, None)
            # Find the end of the contiguous run of this batch_id without
            # row-by-row walking. lag() marks a row whose predecessor is
            # missing or changed batch_id; the first such row at or after
            # start delimits the run. A batch is complete only when that
            # boundary row is actually staged (different/no batch_id). If the
            # run instead ends at the staging tail with no boundary row, the
            # batch is still being delivered and must wait.
            boundary = self._conn.execute(
                """
                WITH marked AS (
                    SELECT seq, batch_id,
                        lag(seq) OVER (ORDER BY seq) AS prev_seq,
                        lag(batch_id) OVER (ORDER BY seq) AS prev_batch
                    FROM event_staging
                    WHERE seq >= ?
                )
                SELECT seq, batch_id FROM marked
                WHERE prev_seq IS DISTINCT FROM seq - 1
                   OR prev_batch IS DISTINCT FROM batch_id
                ORDER BY seq LIMIT 2
                """,
                [start],
            ).fetchall()
            # First row of this region is `start` itself (no predecessor in
            # window); the second row is the run boundary that follows it.
            if len(boundary) < 2:
                return None  # run reaches the staged tail; batch incomplete
            boundary_seq, boundary_batch = boundary[1]
            if boundary_batch == batch_id:
                # A gap inside the batch (missing seq): cannot commit yet.
                return None
            end = boundary_seq - 1
            return CommitGroup(start, end, batch_id)

    def next_staged_seq(self, seq: int) -> tuple[int, str | None] | None:
        """Return (seq, batch_id) for a staged row, or None if not staged."""
        with self._lock:
            row = self._conn.execute(
                "SELECT seq, batch_id FROM event_staging WHERE seq = ?", [seq]
            ).fetchone()
        if row is None:
            return None
        return int(row[0]), row[1]

    # ----------------------------------------------------------------- commits
    def compact_staging(self) -> None:
        """Rewrite event_staging in seq order.

        Pages arrive shuffled, so append-only staging drifts away from seq
        order over time. Range scans driving the delta then degrade into
        scattered I/O. Rebuilding the table in primary-key order (one set
        operation inside DuckDB) restores locality; it is invoked only at
        major boundaries (before a same-batch revision), never per page.
        """
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    f"CREATE TABLE _staging_compact ({STAGING_COLUMNS})"
                )
                conn.execute(
                    "INSERT INTO _staging_compact "
                    "SELECT * FROM event_staging ORDER BY seq"
                )
                conn.execute("DROP TABLE event_staging")
                conn.execute("ALTER TABLE _staging_compact RENAME TO event_staging")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("CHECKPOINT")

    def apply_range(self, start_seq: int, end_seq: int, batch_id: str | None,
                    *, force_checkpoint: bool = False) -> None:
        """Atomically fold staging rows [start_seq, end_seq] into shot_state.

        All set-based processing happens inside DuckDB; the Python process
        passes only the two seq boundaries, so a 100k-record batch is never
        materialized in Python memory. The WAL makes every commit durable
        across process kills; an explicit CHECKPOINT is only forced after a
        same-batch revision to keep the main file current at major boundaries.
        """
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DROP TABLE IF EXISTS _delta")
                conn.execute(
                    _DELTA_GROUP_SQL.format(latest_seqs=_LATEST_SEQS,
                                            join_selects=_JOIN_SELECTS,
                                            joins=_JOIN_CLAUSES,
                                            ).replace("__EVENT_TABLE__", "event_staging"),
                    {"start_seq": start_seq, "end_seq": end_seq},
                )
                # 1) merge into shots that already exist
                set_clauses = ", ".join(
                    f"{a} = CASE WHEN d.{a}_changed THEN d.d_{a} ELSE t.{a} END"
                    for a in _ATTRS
                )
                conn.execute(f"""
                    UPDATE shot_state AS t
                    SET version = d.dv,
                        last_event_seq = d.dlast,
                        last_ts = d.dts,
                        {set_clauses},
                        withdrawn = CASE WHEN d.dwd IS NULL THEN t.withdrawn
                                         ELSE d.dwd END
                    FROM _delta AS d
                    WHERE t.film_id = d.film_id AND t.shot_id = d.shot_id
                """)
                # 2) insert brand-new shots
                conn.execute("""
                    INSERT INTO shot_state (
                        film_id, shot_id, version, last_event_seq, last_ts,
                        title, description, duration_ms, scene, camera,
                        repair_note, withdrawn
                    )
                    SELECT
                        d.film_id, d.shot_id, d.dv, d.dlast, d.dts,
                        d.d_title, d.d_description, d.d_duration_ms, d.d_scene,
                        d.d_camera, d.d_repair_note, COALESCE(d.dwd, false)
                    FROM _delta d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM shot_state t
                        WHERE t.film_id = d.film_id AND t.shot_id = d.shot_id
                    )
                """)
                conn.execute("DROP TABLE _delta")
                conn.execute("""
                    UPDATE sync_state
                    SET last_snapshot_seq = ?,
                        updated_at = strftime(now(), '%Y-%m-%dT%H:%M:%SZ')
                    WHERE id = 1
                """, [end_seq])
                conn.execute("""
                    INSERT INTO catalog_meta VALUES
                    ('last_batch_id', ?),
                    ('last_commit_at', strftime(now(), '%Y-%m-%dT%H:%M:%SZ'))
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, [batch_id or ""])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            if force_checkpoint or batch_id is not None:
                # Flush the WAL so a kill -9 immediately afterwards still
                # leaves coherent on-disk state to resume from. Ordinary
                # commits rely on the WAL itself, which is equally durable.
                conn.execute("CHECKPOINT")

    # ------------------------------------------------------------- verification
    def rebuild_mismatch_count(self) -> int:
        """Test oracle: compare incremental state with a full-prefix rebuild.

        The rebuild reads the full hot+cold history view when archives exist,
        so the oracle still covers history that has already moved to cold.
        """
        with self._lock:
            seq = self.last_snapshot_seq()
            table = self._history_table()
            self._conn.execute("DROP TABLE IF EXISTS _rebuild")
            self._conn.execute(
                _REBUILD_SQL.format(latest_seqs=_REBUILD_LATEST_SEQS,
                                    selects=_REBUILD_SELECTS,
                                    joins=_JOIN_CLAUSES,
                                    ).replace("__EVENT_TABLE__", table),
                {"last_seq": seq},
            )
            mismatches = self._conn.execute("""
                SELECT count(*) FROM (
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM shot_state
                    EXCEPT
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM _rebuild
                    UNION ALL
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM _rebuild
                    EXCEPT
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM shot_state
                )
            """).fetchone()[0]
            self._conn.execute("DROP TABLE IF EXISTS _rebuild")
            return int(mismatches)

    # ----------------------------------------------------------------- queries
    def snapshot_seq(self) -> int:
        return self.last_snapshot_seq()

    def get_shot(self, film_id: str, shot_id: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM catalog_current WHERE film_id = ? AND shot_id = ?",
                [film_id, shot_id],
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return _row_to_dict(cols, row)

    def list_film_shots(self, film_id: str) -> dict[str, Any]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM catalog_current WHERE film_id = ? ORDER BY shot_id",
                [film_id],
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            snapshot = self._conn.execute(
                "SELECT last_snapshot_seq FROM sync_state WHERE id = 1"
            ).fetchone()[0]
            shots = [_row_to_dict(cols, row) for row in rows]
        return {
            "film_id": film_id,
            "snapshot_seq": int(snapshot),
            "count": len(shots),
            "shots": shots,
        }

    def stats(self) -> dict[str, Any]:
        with self._lock:
            staged = self._conn.execute("SELECT count(*), max(seq) FROM event_staging").fetchone()
            visible = self._conn.execute("SELECT count(*) FROM catalog_current").fetchone()[0]
            films = self._conn.execute(
                "SELECT count(DISTINCT film_id) FROM catalog_current"
            ).fetchone()[0]
            snapshot = self._conn.execute(
                "SELECT last_snapshot_seq FROM sync_state WHERE id = 1"
            ).fetchone()[0]
            cold_segments = int(self._conn.execute(
                "SELECT count(*) FROM cold_archive_state"
            ).fetchone()[0])
            cold_events = int(self._conn.execute(
                "SELECT coalesce(sum(row_count), 0) FROM cold_archive_state"
            ).fetchone()[0])
            archived_through = self.archived_through_seq()
        out = {
            "snapshot_seq": int(snapshot),
            "staged_events": int(staged[0]),
            "max_staged_seq": int(staged[1] or 0),
            "visible_shots": int(visible),
            "films": int(films),
            "cold_enabled": self.cold is not None,
            "cold_available": self.cold_available(),
            "cold_segments": cold_segments,
            "cold_events": cold_events,
            "archived_through_seq": archived_through,
            "fetch_watermark_seq": self.fetch_watermark(),
        }
        return out


def _staging_row_to_raw(row: dict[str, Any]) -> dict[str, Any]:
    """Reverse the staging columns back into a normalized raw log record."""
    rec: dict[str, Any] = {
        "seq": int(row["seq"]),
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "film_id": row["film_id"],
        "shot_id": row["shot_id"],
        "version": int(row["version"]),
        "ts": row["ts"],
    }
    if row["batch_id"] is not None:
        rec["batch_id"] = row["batch_id"]
    if row["event_type"] == "upsert":
        payload: dict[str, Any] = {}
        for attr in _ATTRS:
            if row[f"{attr}_present"]:
                payload[attr] = row[attr]
        rec["payload"] = payload
    return rec


def _row_to_dict(cols: list[str], row: tuple) -> dict[str, Any]:
    out = dict(zip(cols, row))
    for key in ("duration_ms", "last_event_seq", "version"):
        if out.get(key) is not None:
            out[key] = int(out[key])
    return out
