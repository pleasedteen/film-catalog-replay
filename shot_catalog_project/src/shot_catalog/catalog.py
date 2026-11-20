"""DuckDB-backed durable shot catalog.

Storage model
=============
* ``event_staging`` -- every normalized source record keyed by source seq.
  Staging is an append-only, idempotent path: in-page duplicates are folded in
  Python (only the current page is buffered) and cross-page redelivery is
  discarded by a primary-key anti-join, so Python never holds more than one
  fetched page at a time and never assembles a whole batch.

* ``shot_state`` -- one row per shot ever seen, holding its current
  attributes and a ``withdrawn`` flag. ``catalog_current`` is a VIEW of the
  visible rows (``NOT withdrawn``), so a withdrawn shot simply disappears from
  every query and reappears on republish.

* ``sync_state`` -- the single durable progress pointer
  (``last_snapshot_seq``), updated in the SAME transaction as the state it
  describes. Saved progress and query answers therefore always refer to the
  same complete prefix of source history.

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

# With sparse subscriptions, staged rows for films that are not actively
# published (unsubscribed films, and newly subscribed films still catching up
# their history) must never leak into the published state. When the
# subscription table is empty -- the original "sync everything" mode -- this
# predicate is constant-true and the behavior is unchanged.
_ACTIVE_SCOPE = """
    (
        NOT EXISTS (SELECT 1 FROM film_subscriptions)
        OR film_id IN (SELECT film_id FROM film_subscriptions WHERE status = 'active')
    )
"""

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
    FROM event_staging
    WHERE {where}
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
    f"LEFT JOIN event_staging {a}e "
    f"ON {a}e.seq = g.{a}_seq AND {a}e.film_id = g.film_id AND {a}e.shot_id = g.shot_id"
    for a in _ATTRS
)
_JOIN_SELECTS = ",\n    ".join(
    f"{a}e.{a} AS d_{a}, (g.{a}_seq IS NOT NULL) AS {a}_changed" for a in _ATTRS
)

# Delta for one seq range, restricted to films that are currently published.
# In sparse mode a backfilling film is hidden until its history reaches the
# published snapshot, so ordinary groups must not touch it; an unsubscribed
# film is never in the table at all. With no subscriptions the predicate is
# constant-true, matching the original "sync everything" behavior.
_DELTA_WHERE = f"seq BETWEEN $start_seq AND $end_seq AND {_ACTIVE_SCOPE}"
_REBUILD_WHERE = f"seq <= $last_seq AND {_ACTIVE_SCOPE}"

# Scoped to a single film: backfill folds that film's staged history into
# shot_state without publishing it (the view hides films whose subscription is
# still 'backfill').
_FILM_DELTA_WHERE = (
    "seq > $start_seq AND seq <= $end_seq AND film_id = $film_id"
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
    FROM event_staging
    WHERE {where}
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


class Catalog:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        # One connection guarded by an RLock: DuckDB disallows mixing
        # read-only and read-write connections to one file in a process.
        # Reads are short autocommit statements, so a film listing observes
        # one coherent MVCC snapshot while the sync thread waits its turn.
        self._lock = threading.RLock()
        self._conn = duckdb.connect(self.db_path)
        self._stage_ready = False
        self._init_schema()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        conn = self._conn
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(f"CREATE TABLE IF NOT EXISTS event_staging ({STAGING_COLUMNS})")
            conn.execute(f"CREATE TABLE IF NOT EXISTS shot_state ({STATE_COLUMNS})")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    last_snapshot_seq BIGINT NOT NULL,
                    updated_at VARCHAR NOT NULL,
                    CHECK (id = 1)
                )
            """)
            # Fetch watermark: highest seq whose source window has been fully
            # accounted for (staged if subscribed, deliberately skipped if not).
            # Nullable-then-filled so an existing dense database migrates.
            conn.execute(
                "ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS resolved_seq BIGINT"
            )
            conn.execute(
                "UPDATE sync_state SET resolved_seq = last_snapshot_seq "
                "WHERE resolved_seq IS NULL"
            )
            conn.execute(
                "ALTER TABLE sync_state ALTER COLUMN resolved_seq SET DEFAULT 0"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS film_subscriptions (
                    film_id VARCHAR PRIMARY KEY,
                    status VARCHAR NOT NULL,          -- 'backfill' | 'active'
                    subscribed_at VARCHAR NOT NULL,
                    backfill_fetch_seq BIGINT NOT NULL DEFAULT 0,
                    backfilled_seq BIGINT NOT NULL DEFAULT 0,
                    activated_at VARCHAR
                )
            """)
            # Learned full extents of same-batch revisions. The source reports
            # them whenever an intersecting page is fetched, so a sparse reader
            # knows a batch's end without transferring its whole content.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_extents (
                    batch_id VARCHAR NOT NULL,
                    start_seq BIGINT NOT NULL,
                    end_seq BIGINT NOT NULL,
                    PRIMARY KEY (batch_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS catalog_meta (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR
                )
            """)
            conn.execute("""
                INSERT INTO sync_state (id, last_snapshot_seq, resolved_seq, updated_at)
                SELECT 1, 0, 0, strftime(now(), '%Y-%m-%dT%H:%M:%SZ')
                WHERE NOT EXISTS (SELECT 1 FROM sync_state)
            """)
            # Recreate (not IF NOT EXISTS) so a database from the pre-subscription
            # schema picks up the subscription visibility rule on reopen.
            conn.execute("DROP VIEW IF EXISTS catalog_current")
            conn.execute("""
                CREATE VIEW catalog_current AS
                SELECT film_id, shot_id, version, last_event_seq, last_ts,
                       title, description, duration_ms, scene, camera, repair_note
                FROM shot_state
                WHERE NOT withdrawn
                  AND (
                    NOT EXISTS (SELECT 1 FROM film_subscriptions)
                    OR film_id IN (
                        SELECT film_id FROM film_subscriptions WHERE status = 'active'
                    )
                  )
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

    def resolved_seq(self) -> int:
        """Highest global seq the source has fully accounted for."""
        with self._lock:
            return int(
                self._conn.execute(
                    "SELECT COALESCE(resolved_seq, last_snapshot_seq) "
                    "FROM sync_state WHERE id = 1"
                ).fetchone()[0]
            )

    def mark_resolved(self, seq: int) -> None:
        """Advance the fetch watermark (never backwards)."""
        with self._lock:
            self._conn.execute("""
                UPDATE sync_state
                SET resolved_seq = GREATEST(COALESCE(resolved_seq, 0), ?)
                WHERE id = 1
            """, [seq])

    # ----------------------------------------------------------- subscriptions
    def has_subscriptions(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT count(*) FROM film_subscriptions"
            ).fetchone()
        return int(row[0]) > 0

    def subscribed_films(self) -> list[str]:
        """All films the source should deliver (backfill + active)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT film_id FROM film_subscriptions ORDER BY film_id"
            ).fetchall()
        return [r[0] for r in rows]

    def active_films(self) -> list[str]:
        """Films currently published. Backfilling films are driven separately."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT film_id FROM film_subscriptions "
                "WHERE status = 'active' ORDER BY film_id"
            ).fetchall()
        return [r[0] for r in rows]

    def add_subscription(self, film_id: str) -> str:
        """Subscribe a film. Returns its resulting status ('active'|'backfill').

        A film added while nothing has been published yet (global snapshot 0)
        has no prior history to fill, so it starts 'active' and simply joins the
        normal scan. A film added later starts 'backfill' and stays invisible
        until its history catches up to the published snapshot. Re-subscribing
        an existing film is a no-op, preserving in-flight/complete progress.
        """
        with self._lock:
            conn = self._conn
            row = conn.execute(
                "SELECT status FROM film_subscriptions WHERE film_id = ?",
                [film_id],
            ).fetchone()
            if row is not None:
                return row[0]
            if self.last_snapshot_seq() == 0:
                # Nothing has been published yet, so the film joins the normal
                # scan as 'active' -- there is no earlier version to fill.
                conn.execute("""
                    INSERT INTO film_subscriptions
                        (film_id, status, subscribed_at, backfill_fetch_seq,
                         backfilled_seq, activated_at)
                    VALUES (?, 'active', strftime(now(), '%Y-%m-%dT%H:%M:%SZ'),
                            0, 0, strftime(now(), '%Y-%m-%dT%H:%M:%SZ'))
                """, [film_id])
                return "active"
            conn.execute("""
                INSERT INTO film_subscriptions
                    (film_id, status, subscribed_at, backfill_fetch_seq,
                     backfilled_seq, activated_at)
                VALUES (?, 'backfill', strftime(now(), '%Y-%m-%dT%H:%M:%SZ'),
                        0, 0, NULL)
            """, [film_id])
            return "backfill"

    def subscription_status(self, film_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM film_subscriptions WHERE film_id = ?",
                [film_id],
            ).fetchone()
        return None if row is None else row[0]

    def subscription_rows(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("""
                SELECT film_id, status, subscribed_at, backfill_fetch_seq,
                       backfilled_seq, activated_at
                FROM film_subscriptions ORDER BY film_id
            """).fetchall()
        return [{
            "film_id": r[0], "status": r[1], "subscribed_at": r[2],
            "backfill_fetch_seq": int(r[3]), "backfilled_seq": int(r[4]),
            "activated_at": r[5],
        } for r in rows]

    def backfill_films(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT film_id FROM film_subscriptions "
                "WHERE status = 'backfill' ORDER BY film_id"
            ).fetchall()
        return [r[0] for r in rows]

    def backfill_progress(self, film_id: str) -> tuple[int, int]:
        """(fetch_cursor, folded_seq) for a backfilling film."""
        with self._lock:
            row = self._conn.execute(
                "SELECT backfill_fetch_seq, backfilled_seq "
                "FROM film_subscriptions WHERE film_id = ?",
                [film_id],
            ).fetchone()
        return (0, 0) if row is None else (int(row[0]), int(row[1]))

    def advance_backfill_fetch(self, film_id: str, seq: int) -> None:
        with self._lock:
            self._conn.execute("""
                UPDATE film_subscriptions
                SET backfill_fetch_seq = GREATEST(backfill_fetch_seq, ?)
                WHERE film_id = ?
            """, [seq, film_id])

    def try_activate_film(self, film_id: str) -> bool:
        """Flip a fully-backfilled film to 'active' atomically.

        The film becomes queryable only when its folded history covers the
        current published snapshot -- i.e. it is at the same published version
        as the already-active subscriptions. This never folds by itself: callers
        must have staged and folded the film's history up to the snapshot first
        (``backfilled_seq >= last_snapshot_seq``). Doing the check and the flip
        under the catalog lock means the snapshot cannot slip in between.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT status, backfilled_seq FROM film_subscriptions "
                "WHERE film_id = ?",
                [film_id],
            ).fetchone()
            if row is None or row[0] != "backfill":
                return False
            if int(row[1]) < self.last_snapshot_seq():
                return False
            self._conn.execute("""
                UPDATE film_subscriptions
                SET status = 'active',
                    activated_at = strftime(now(), '%Y-%m-%dT%H:%M:%SZ')
                WHERE film_id = ?
            """, [film_id])
            return True

    # ------------------------------------------------------ batch extents
    def record_batch_runs(self, runs: Iterable[dict]) -> None:
        """Persist learned full extents of same-batch revisions.

        Idempotent: the source reports the same extent on every intersecting
        page, and re-learning it must never fail or duplicate.
        """
        rows = [(r["batch_id"], int(r["start_seq"]), int(r["end_seq"]))
                for r in runs if r.get("batch_id")]
        if not rows:
            return
        with self._lock:
            conn = self._conn
            conn.execute(
                "CREATE TEMP TABLE IF NOT EXISTS _runs_in ("
                "batch_id VARCHAR, start_seq BIGINT, end_seq BIGINT)"
            )
            conn.execute("TRUNCATE TABLE _runs_in")
            with conn.appender("_runs_in") as appender:
                for row in rows:
                    appender.append_row(*row)
            conn.execute("""
                INSERT INTO batch_extents (batch_id, start_seq, end_seq)
                SELECT DISTINCT batch_id, start_seq, end_seq FROM _runs_in
                ON CONFLICT (batch_id) DO UPDATE SET
                    start_seq = LEAST(batch_extents.start_seq, EXCLUDED.start_seq),
                    end_seq = GREATEST(batch_extents.end_seq, EXCLUDED.end_seq)
            """)

    def batch_extent(self, batch_id: str) -> tuple[int, int] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT start_seq, end_seq FROM batch_extents WHERE batch_id = ?",
                [batch_id],
            ).fetchone()
        return None if row is None else (int(row[0]), int(row[1]))

    def batch_extent_covering(self, seq: int) -> tuple[str, int, int] | None:
        """Extent of whichever batch contains ``seq`` (learned from its pages)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT batch_id, start_seq, end_seq FROM batch_extents "
                "WHERE start_seq <= ? AND end_seq >= ? LIMIT 1",
                [seq, seq],
            ).fetchone()
        return None if row is None else (row[0], int(row[1]), int(row[2]))

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

    def next_batch_start_after(self, seq: int) -> int | None:
        """Smallest known batch start strictly greater than ``seq``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT min(start_seq) FROM batch_extents WHERE start_seq > ?",
                [seq],
            ).fetchone()
        return None if row[0] is None else int(row[0])

    def next_sparse_group(self) -> CommitGroup | None:
        """Next publishable global prefix group in sparse mode.

        The published snapshot advances over every global seq the source has
        resolved, even where no subscribed film has an event. Two constraints
        shape the group:

        * a same-batch revision is one atomic group spanning its full learned
          extent, and may only be published once the whole extent is resolved
          (so the subscribed survivors are all staged) and it attaches exactly
          to the published prefix;
        * an ordinary run never crosses into a known batch, so a batch is
          always published as a whole.

        Returns ``None`` when nothing beyond the snapshot has been resolved yet
        (the caller then waits/fetches more).
        """
        with self._lock:
            start = self.last_snapshot_seq() + 1
            resolved = self.resolved_seq()
            if start > resolved:
                return None
            covering = self.batch_extent_covering(start)
            if covering is not None:
                batch_id, bstart, bend = covering
                if bstart != start:
                    # The published prefix would split the batch; wait until it
                    # attaches exactly (should not happen for a correct walk).
                    return None
                if bend > resolved:
                    return None  # whole batch not yet accounted for
                return CommitGroup(bstart, bend, batch_id)
            # Ordinary run up to the resolved watermark, stopping just before
            # the next known batch so a batch is never torn.
            end = resolved
            nb = self.next_batch_start_after(start)
            if nb is not None and nb <= end:
                end = nb - 1
            return CommitGroup(start, end, None)

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

        Dense "sync everything" path: every staged row in the range applies.
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
                self._apply_delta(start_seq, end_seq,
                                  "seq BETWEEN $start_seq AND $end_seq")
                conn.execute("""
                    UPDATE sync_state
                    SET last_snapshot_seq = ?,
                        resolved_seq = GREATEST(COALESCE(resolved_seq, 0), ?),
                        updated_at = strftime(now(), '%Y-%m-%dT%H:%M:%SZ')
                    WHERE id = 1
                """, [end_seq, end_seq])
                self._record_commit_meta(batch_id)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            if force_checkpoint or batch_id is not None:
                # Flush the WAL so a kill -9 immediately afterwards still
                # leaves coherent on-disk state to resume from. Ordinary
                # commits rely on the WAL itself, which is equally durable.
                conn.execute("CHECKPOINT")

    def apply_publication(self, start_seq: int, end_seq: int) -> None:
        """Sparse path: publish (start,end] for currently *active* films.

        Records of unsubscribed films are not staged at all; records of
        backfilling films are deliberately excluded by the active scope so a
        half-caught-up film can never leak into the published snapshot. The
        published snapshot advances across the whole global range even where
        no subscribed film has an event -- that is what 'the snapshot keeps
        advancing' means in sparse mode.
        """
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN TRANSACTION")
            try:
                self._apply_delta(start_seq, end_seq, _DELTA_WHERE)
                conn.execute("""
                    UPDATE sync_state
                    SET last_snapshot_seq = ?,
                        resolved_seq = GREATEST(COALESCE(resolved_seq, 0), ?),
                        updated_at = strftime(now(), '%Y-%m-%dT%H:%M:%SZ')
                    WHERE id = 1
                """, [end_seq, end_seq])
                self._record_commit_meta(None)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def fold_film_range(self, film_id: str, start_seq: int, end_seq: int) -> None:
        """Fold one film's staged rows (start,end] into its hidden state.

        Used only for backfill: the film is still 'backfill' so the view hides
        it, and folding is done in increasing seq order so partial revisions
        compose correctly. No snapshot pointer is touched -- backfill does not
        advance the global published version.
        """
        if end_seq <= start_seq:
            return
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN TRANSACTION")
            try:
                self._apply_delta(start_seq, end_seq, _FILM_DELTA_WHERE,
                                  {"film_id": film_id})
                conn.execute("""
                    UPDATE film_subscriptions
                    SET backfilled_seq = ?
                    WHERE film_id = ? AND backfilled_seq < ?
                """, [end_seq, film_id, end_seq])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _apply_delta(self, start_seq: int, end_seq: int, where_sql: str,
                     extra_params: dict | None = None) -> None:
        """Build the per-shot delta for a seq range and merge it into state.

        Runs inside the caller's open transaction.
        """
        conn = self._conn
        params: dict[str, Any] = {"start_seq": start_seq, "end_seq": end_seq}
        if extra_params:
            params.update(extra_params)
        conn.execute("DROP TABLE IF EXISTS _delta")
        conn.execute(
            _DELTA_GROUP_SQL.format(latest_seqs=_LATEST_SEQS,
                                    join_selects=_JOIN_SELECTS,
                                    joins=_JOIN_CLAUSES, where=where_sql),
            params,
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

    def _record_commit_meta(self, batch_id: str | None) -> None:
        self._conn.execute("""
            INSERT INTO catalog_meta VALUES
            ('last_batch_id', ?),
            ('last_commit_at', strftime(now(), '%Y-%m-%dT%H:%M:%SZ'))
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, [batch_id or ""])

    # ------------------------------------------------------------- verification
    def _published_scope_sql(self, alias: str = "") -> str:
        p = f"{alias}." if alias else ""
        return (f"(NOT EXISTS (SELECT 1 FROM film_subscriptions) OR "
                f"{p}film_id IN (SELECT film_id FROM film_subscriptions "
                f"WHERE status = 'active'))")

    def rebuild_mismatch_count(self) -> int:
        """Test oracle: compare the *published* state with a full-prefix rebuild.

        Both sides are restricted to active films, so a film still backfilling
        (hidden from queries by design) is not mistaken for a mismatch. In
        dense mode there are no subscriptions and the predicate is all-true,
        exactly as before.
        """
        with self._lock:
            seq = self.last_snapshot_seq()
            self._conn.execute("DROP TABLE IF EXISTS _rebuild")
            self._conn.execute(
                _REBUILD_SQL.format(latest_seqs=_REBUILD_LATEST_SEQS,
                                    selects=_REBUILD_SELECTS,
                                    joins=_JOIN_CLAUSES, where=_REBUILD_WHERE),
                {"last_seq": seq},
            )
            scope = self._published_scope_sql()
            mismatches = self._conn.execute(f"""
                SELECT count(*) FROM (
                    SELECT film_id, shot_id, version, last_event_seq, last_ts,
                           title, description, duration_ms, scene, camera,
                           repair_note, withdrawn FROM shot_state
                    WHERE {scope}
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
                    WHERE {scope}
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
            row = self._conn.execute(
                "SELECT last_snapshot_seq, COALESCE(resolved_seq, last_snapshot_seq) "
                "FROM sync_state WHERE id = 1"
            ).fetchone()
            subs = self._conn.execute("""
                SELECT
                    count(*) FILTER (WHERE status = 'active'),
                    count(*) FILTER (WHERE status = 'backfill')
                FROM film_subscriptions
            """).fetchone()
        return {
            "snapshot_seq": int(row[0]),
            "resolved_seq": int(row[1]),
            "staged_events": int(staged[0]),
            "max_staged_seq": int(staged[1] or 0),
            "visible_shots": int(visible),
            "films": int(films),
            "active_subscriptions": int(subs[0]),
            "backfilling_subscriptions": int(subs[1]),
        }


def _row_to_dict(cols: list[str], row: tuple) -> dict[str, Any]:
    out = dict(zip(cols, row))
    for key in ("duration_ms", "last_event_seq", "version"):
        if out.get(key) is not None:
            out[key] = int(out[key])
    return out
