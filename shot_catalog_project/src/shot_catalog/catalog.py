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

Snapshot reads
==============
Writes and reads no longer share a lock. Every query opens its own read
transaction on a DuckDB cursor (an independent connection onto the same
database). DuckDB's MVCC gives that cursor the last *published* snapshot --
the state at ``last_snapshot_seq`` -- even while the sync thread is staging
the next pages or executing a large ``apply_range`` transaction. A paused
cross-page batch therefore never blocks a point query, a film listing or a
health check; they simply answer from the previous published version.

A film listing streams with keyset pagination from a single read
transaction, so the server never materializes the whole answer in Python
memory, later commits can keep publishing while a slow client drains the
response, and closing the cursor (on completion or client disconnect)
releases the pinned snapshot immediately. ``CHECKPOINT`` is best-effort:
an open reader transaction may veto it, and the WAL is durable regardless.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

import duckdb

from .log_format import NormalizedRecord

log = logging.getLogger("shot_catalog.catalog")

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
    FROM event_staging
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
    f"LEFT JOIN event_staging {a}e "
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
    FROM event_staging
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


class Catalog:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        # The writer connection serializes all mutations (the sync thread is
        # the only writer). Reads NEVER use it: each query opens its own
        # DuckDB cursor, which is an independent connection onto the same
        # database and therefore gets its own MVCC snapshot without waiting
        # for the writer. That is what keeps point queries, film listings and
        # health checks answerable while a cross-page batch is still being
        # committed (it answers from the previously published snapshot).
        self._lock = threading.RLock()
        self._conn = duckdb.connect(self.db_path)
        self._stage_ready = False
        # Guards CHECKPOINT attempts: a checkpoint may fail while any reader
        # transaction is open. Failures are harmless (the WAL is durable) but
        # we avoid logging a warning for the expected contention case.
        self._checkpoint_lock = threading.Lock()
        # Number of request snapshots currently pinned. Exposed so operators
        # (and tests) can see that a disconnected client's snapshot was
        # released rather than leaked.
        self._readers = 0
        self._readers_lock = threading.Lock()
        self._init_schema()

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

    # ------------------------------------------------------------ read snapshots
    @contextlib.contextmanager
    def read_snapshot(self) -> Iterator["ReadSnapshot"]:
        """Open one consistent, read-only snapshot for a single request.

        The cursor is an independent DuckDB connection, so it starts a fresh
        read transaction reading the latest *committed* state and is not
        blocked by (nor does it block) an in-flight writer transaction. The
        caller must not keep it past the request: closing releases the pinned
        snapshot so later commits can be cleaned up.
        """
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN TRANSACTION")
        except Exception:
            cur.close()
            raise
        with self._readers_lock:
            self._readers += 1
        try:
            yield ReadSnapshot(cur)
        finally:
            try:
                cur.execute("COMMIT")
            except Exception:  # pragma: no cover - defensive close
                pass
            cur.close()
            with self._readers_lock:
                self._readers -= 1

    @property
    def active_readers(self) -> int:
        """Number of request snapshots currently pinned."""
        with self._readers_lock:
            return self._readers

    def _checkpoint_best_effort(self) -> None:
        """Try to flush the WAL; an open reader transaction may veto it."""
        with self._checkpoint_lock:
            try:
                self._conn.execute("CHECKPOINT")
            except duckdb.Error as exc:  # active reader transaction, or WAL busy
                log.debug("checkpoint deferred: %s", exc)

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

    def ordinary_run_end(self, start_seq: int, limit_seq: int) -> int:
        """Last seq of the contiguous ordinary run that starts at ``start_seq``.

        The run may not exceed ``limit_seq`` and ends just before the first
        unstaged seq, the first row carrying a ``batch_id``, or the limit.
        Evaluated in one set-based statement instead of a per-seq Python walk,
        so coalescing a 100k-record prefix costs a handful of queries rather
        than 100k round trips.
        """
        with self._lock:
            row = self._conn.execute(
                """
                WITH r AS (
                    SELECT seq, batch_id,
                           row_number() OVER (ORDER BY seq) AS rn
                    FROM event_staging
                    WHERE seq BETWEEN $start AND $lim
                )
                SELECT coalesce(min(seq) FILTER (
                           WHERE batch_id IS NOT NULL
                              OR seq <> $start + rn - 1), $lim + 1) - 1
                       AS first_break,
                       max(seq) AS last_staged
                FROM r
                """,
                {"start": start_seq, "lim": limit_seq},
            ).fetchone()
        first_break = int(row[0]) if row[0] is not None else limit_seq
        last_staged = int(row[1]) if row[1] is not None else start_seq
        # first_break is the last seq before the break; never past the staged
        # tail, since seqs beyond it are simply absent from the window.
        return max(start_seq, min(first_break, last_staged))

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
            self._checkpoint_best_effort()

    def apply_range(self, start_seq: int, end_seq: int, batch_id: str | None,
                    *, force_checkpoint: bool = False,
                    before_commit: Callable[[], None] | None = None) -> None:
        """Atomically fold staging rows [start_seq, end_seq] into shot_state.

        All set-based processing happens inside DuckDB; the Python process
        passes only the two seq boundaries, so a 100k-record batch is never
        materialized in Python memory. The WAL makes every commit durable
        across process kills; an explicit CHECKPOINT is only forced after a
        same-batch revision to keep the main file current at major boundaries.

        ``before_commit`` (test/demo hook) runs while the transaction is still
        uncommitted. That models the worst case for readers: a large
        cross-page revision has done all its work but has not published yet.
        """
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("DROP TABLE IF EXISTS _delta")
                conn.execute(
                    _DELTA_GROUP_SQL.format(latest_seqs=_LATEST_SEQS,
                                            join_selects=_JOIN_SELECTS,
                                            joins=_JOIN_CLAUSES),
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
                if before_commit is not None:
                    before_commit()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            if force_checkpoint or batch_id is not None:
                # Flush the WAL so a kill -9 immediately afterwards still
                # leaves coherent on-disk state to resume from. Ordinary
                # commits rely on the WAL itself, which is equally durable.
                # An active reader transaction may refuse the flush; that is
                # safe (WAL replay recovers it) and must not fail the commit.
                self._checkpoint_best_effort()

    # ------------------------------------------------------------- verification
    def rebuild_mismatch_count(self) -> int:
        """Test oracle: compare incremental state with a full-prefix rebuild."""
        with self._lock:
            seq = self.last_snapshot_seq()
            self._conn.execute("DROP TABLE IF EXISTS _rebuild")
            self._conn.execute(
                _REBUILD_SQL.format(latest_seqs=_REBUILD_LATEST_SEQS,
                                    selects=_REBUILD_SELECTS,
                                    joins=_JOIN_CLAUSES),
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
        # Read through a snapshot, not the writer connection: observing the
        # published pointer must not wait behind an in-flight apply_range.
        with self.read_snapshot() as snap:
            return snap.snapshot_seq()

    def get_shot(self, film_id: str, shot_id: str) -> dict[str, Any] | None:
        with self.read_snapshot() as snap:
            return snap.get_shot(film_id, shot_id)

    def list_film_shots(self, film_id: str) -> dict[str, Any]:
        with self.read_snapshot() as snap:
            return snap.list_film_shots(film_id)

    def stats(self) -> dict[str, Any]:
        with self.read_snapshot() as snap:
            return snap.stats()


class ReadSnapshot:
    """One MVCC snapshot, owned by a single request.

    All accessors read through the same open read transaction, so they see
    one published version of the catalog even if the sync thread commits in
    between calls. ``iter_film_shots`` streams with keyset pagination, which
    keeps Python memory bounded regardless of film size and lets the writer
    publish newer versions while a slow response is being drained.
    """

    # Column order of the rows returned by the shot SELECTs.
    _COLUMNS = (
        "film_id", "shot_id", "version", "last_event_seq", "last_ts",
        "title", "description", "duration_ms", "scene", "camera", "repair_note",
    )
    _SHOT_SQL = (
        "SELECT film_id, shot_id, version, last_event_seq, last_ts, "
        "title, description, duration_ms, scene, camera, repair_note "
        "FROM catalog_current"
    )

    def __init__(self, cur: duckdb.DuckDBPyConnection):
        self._cur = cur

    def snapshot_seq(self) -> int:
        row = self._cur.execute(
            "SELECT last_snapshot_seq FROM sync_state WHERE id = 1"
        ).fetchone()
        return int(row[0])

    def get_shot(self, film_id: str, shot_id: str) -> dict[str, Any] | None:
        row = self._cur.execute(
            self._SHOT_SQL + " WHERE film_id = ? AND shot_id = ?",
            [film_id, shot_id],
        ).fetchone()
        if row is None:
            return None
        return _row_to_dict(self._COLUMNS, row)

    def film_shot_count(self, film_id: str) -> int:
        row = self._cur.execute(
            "SELECT count(*) FROM catalog_current WHERE film_id = ?", [film_id]
        ).fetchone()
        return int(row[0])

    def iter_film_shots(self, film_id: str, *, page_size: int = 1000
                        ) -> Iterator[dict[str, Any]]:
        """Yield the film's visible shots in shot_id order, keyset-paged.

        Every page is read inside the same transaction, so the concatenation
        is one published version of the film. Only ``page_size`` rows live in
        Python at a time; ``shot_id > last`` uses the (film_id, shot_id)
        primary key, so a 100k-shot film costs a bounded amount of memory and
        work per page. If the consumer stops early (client disconnect), the
        generator is closed and the caller's ``read_snapshot`` releases the
        snapshot.
        """
        last: str | None = None
        while True:
            if last is None:
                rows = self._cur.execute(
                    self._SHOT_SQL + " WHERE film_id = ? ORDER BY shot_id LIMIT ?",
                    [film_id, page_size],
                ).fetchall()
            else:
                rows = self._cur.execute(
                    self._SHOT_SQL
                    + " WHERE film_id = ? AND shot_id > ? ORDER BY shot_id LIMIT ?",
                    [film_id, last, page_size],
                ).fetchall()
            if not rows:
                return
            for row in rows:
                yield _row_to_dict(self._COLUMNS, row)
            last = rows[-1][1]
            if len(rows) < page_size:
                return

    def list_film_shots(self, film_id: str) -> dict[str, Any]:
        shots = list(self.iter_film_shots(film_id))
        return {
            "film_id": film_id,
            "snapshot_seq": self.snapshot_seq(),
            "count": len(shots),
            "shots": shots,
        }

    def stats(self) -> dict[str, Any]:
        staged = self._cur.execute(
            "SELECT count(*), max(seq) FROM event_staging"
        ).fetchone()
        visible = self._cur.execute(
            "SELECT count(*) FROM catalog_current"
        ).fetchone()[0]
        films = self._cur.execute(
            "SELECT count(DISTINCT film_id) FROM catalog_current"
        ).fetchone()[0]
        snapshot = self.snapshot_seq()
        return {
            "snapshot_seq": snapshot,
            "staged_events": int(staged[0]),
            "max_staged_seq": int(staged[1] or 0),
            "visible_shots": int(visible),
            "films": int(films),
        }


def _row_to_dict(cols: tuple[str, ...] | list[str], row: tuple) -> dict[str, Any]:
    out = dict(zip(cols, row))
    for key in ("duration_ms", "last_event_seq", "version"):
        if out.get(key) is not None:
            out[key] = int(out[key])
    return out
