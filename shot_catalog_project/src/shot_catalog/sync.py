"""Paged catch-up synchronization engine with hot/cold backpressure.

The engine never trusts delivery order. It repeatedly pulls pages from the
local HTTP log source, normalizes and idempotently merges records into DuckDB
staging, then commits whatever prefix groups are now complete. Memory holds
at most one page at a time -- even a 100k-shot revision spanning thousands of
pages is never assembled in Python.

Cold backpressure
=================
When a cold store is configured, already-published history moves out of the
hot database through an ``archiver`` callback invoked after every commit. If
the cold side fails, publishing may outrun archival by AT MOST ONE new commit
group: once a group sits published-but-unarchived in the hot database and the
cold side is still down, the engine stops paging new source pages and stops
publishing further staged groups. Queries keep answering from the hot
``shot_state`` (they never read the cold side); redelivery of old pages only
touches hot staging and stays harmless. When the cold side recovers, the next
archiver call drains the backlog and paging resumes automatically.

Crash recovery needs no special code beyond durability: every staged page is
committed in DuckDB (its WAL survives process kill), and every catalog commit
advances ``sync_state.last_snapshot_seq`` in the same transaction as the
snapshot swap. On restart the engine simply re-pulls pages (redelivery is
normal) and continues from the persisted snapshot prefix, so saved progress
and query answers always describe the same complete history prefix.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

from .catalog import Catalog, CommitGroup
from .cold import ColdUnavailable
from .log_format import LogFormatError, NormalizedRecord, normalize

log = logging.getLogger("shot_catalog.sync")


class LogSourceClient:
    """Thin urllib client for the local paged log source (no third parties)."""

    def __init__(self, base_url: str, *, timeout: float = 10.0,
                 max_retries: int = 6, backoff: float = 0.02):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff

    def _get_json(self, url: str) -> dict:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code == 404:
                    raise
                # 500/503: transient simulated failures -> retry with backoff
                if exc.code >= 500 and attempt < self.max_retries - 1:
                    time.sleep(self.backoff * (2 ** attempt))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    time.sleep(self.backoff * (2 ** attempt))
                    continue
                raise
        raise RuntimeError(f"exhausted retries for {url}: {last_exc}")

    def health(self) -> dict:
        return self._get_json(f"{self.base_url}/health")

    def total(self) -> int:
        """Cheap source-tail probe: one small /health request, no pages."""
        return int(self.health()["total"])

    def fetch_page(self, offset: int, limit: int) -> dict:
        qs = urllib.parse.urlencode({"offset": offset, "limit": limit})
        return self._get_json(f"{self.base_url}/logs?{qs}")


@dataclass
class SyncCounters:
    pages_fetched: int = 0
    transient_errors: int = 0
    records_seen: int = 0          # including redeliveries
    records_newly_staged: int = 0
    groups_committed: int = 0
    batch_groups_committed: int = 0
    largest_group: int = 0
    last_group: CommitGroup | None = None
    history: list[dict] = field(default_factory=list)
    cold_segments_archived: int = 0
    cold_pause_events: int = 0    # how often cold backpressure paused paging
    cold_outages: int = 0         # distinct observed outage episodes


class SyncEngine:
    def __init__(self, catalog: Catalog, client: LogSourceClient,
                 *, page_size: int = 100, poll_interval: float = 0.15,
                 sleep: Callable[[float], None] = time.sleep,
                 on_commit: Callable[[CommitGroup], None] | None = None,
                 archiver: Callable[[CommitGroup], object] | None = None,
                 cold_backlog_limit: int = 1):
        self.catalog = catalog
        self.client = client
        self.page_size = page_size
        self.poll_interval = poll_interval
        self._sleep = sleep
        # Invoked after every durable commit group, while holding no engine
        # lock; lets a supervisor observe/act on each prefix advancement.
        self.on_commit = on_commit
        # Optional cold-side callback; ``make_archiver`` builds the default one
        # that moves each published group's hot rows into Parquet segments.
        self.archiver = archiver
        # At most this many published-but-unarchived groups may accumulate
        # while the cold side is down before paging/publishing pause.
        self.cold_backlog_limit = max(1, int(cold_backlog_limit))
        self._stop = False
        self._cold_was_down = False
        self.counters = SyncCounters()

    def stop(self) -> None:
        self._stop = True

    # ------------------------------------------------------- cold backpressure
    def cold_paused(self) -> bool:
        """True when cold backpressure currently blocks further progress.

        With no archiver configured there is no cold side to back up against;
        with one configured, paging pauses as soon as a published group is
        waiting unarchived and the cold side is unavailable.
        """
        if self.archiver is None:
            return False
        backlog = self.catalog.hot_published_group_count()
        if backlog <= 0:
            return False
        if self.catalog.cold_available():
            return False
        return True

    def _try_archive(self, group: CommitGroup) -> bool:
        """Drive the cold side after a commit. Returns True on success/no-op."""
        if self.archiver is None:
            return True
        if self.catalog.cold_available() and self._cold_was_down:
            log.info("cold side recovered; catching up archived backlog")
            self._cold_was_down = False
        try:
            result = self.archiver(group)
            if result is not None:
                self.counters.cold_segments_archived += 1
            return True
        except ColdUnavailable as exc:
            if not self._cold_was_down:
                self.counters.cold_outages += 1
                self._cold_was_down = True
            log.warning("cold archival paused: %s", exc)
            return False

    # ------------------------------------------------------------- one pass
    def _pull_and_stage(self, start_offset: int = 0) -> int:
        """Fetch pages from ``start_offset`` to the source tail; return total."""
        offset = start_offset
        total = None
        while not self._stop:
            if self.cold_paused():
                # Cold outage + one group already waiting: stop paging. Old
                # pages are never re-fetched here, and unpublished staged data
                # stays committed in hot staging untouched.
                self.counters.cold_pause_events += 1
                log.warning("paging paused: cold backlog at its limit")
                break
            try:
                page = self.client.fetch_page(offset, self.page_size)
            except urllib.error.HTTPError as exc:
                if exc.code >= 500:
                    self.counters.transient_errors += 1
                raise
            self.counters.pages_fetched += 1
            total = page["total"]
            raw_records = page["records"]
            normalized: list[NormalizedRecord] = []
            for raw in raw_records:
                try:
                    normalized.append(normalize(raw))
                except LogFormatError:
                    log.exception("malformed record skipped at offset %s", offset)
                    continue
            self.counters.records_seen += len(raw_records)
            staged = self.catalog.stage_records(normalized)
            self.counters.records_newly_staged += staged
            # Durable fetch watermark: even pages whose seqs were already
            # archived extend the region known to have been delivered, so the
            # resume offset never collapses back to 0 after hot rows move cold.
            max_seq = max((r.seq for r in normalized), default=0)
            if max_seq:
                self.catalog.record_fetch_watermark(max_seq)
            if not page["has_more"]:
                break
            # Overlapping pages are allowed; advance by page_size regardless.
            offset += self.page_size
            if offset >= total:
                break
        return total or 0

    def _commit_ready_groups(self, ordinary_flush_size: int = 5000,
                             max_groups: int | None = None) -> None:
        """Commit every prefix group that is fully staged.

        Same-batch revisions are always applied as their own atomic group.
        Consecutive ordinary records are coalesced into one range apply per
        ``ordinary_flush_size`` records so a 100k-event prefix costs a few
        hundred transactions instead of 100k -- while still advancing the
        durable snapshot only through complete, contiguous history.

        When a cold store is configured and the cold side is down, at most one
        further group is published; the loop then parks until the cold side
        recovers and the archiver drains the backlog. ``max_groups`` optionally
        stops after that many commits (used by tests that need an intermediate
        published prefix).
        """
        committed = 0
        while not self._stop:
            if self.cold_paused():
                return
            if max_groups is not None and committed >= max_groups:
                return
            group = self.catalog.next_committable_group()
            if group is None:
                return
            if group.batch_id is not None:
                self._apply(group)
                committed += 1
                continue
            # Coalesce ready ordinary records by inspecting the staging rows
            # directly: the flush ends just before a batch boundary or at the
            # first seq that has not been durably staged yet.
            start = group.start_seq
            end = start
            limit = start + ordinary_flush_size - 1
            while end < limit:
                nxt = end + 1
                row = self.catalog.next_staged_seq(nxt)
                if row is None:
                    break  # not staged yet (or source tail reached)
                nxt_seq, nxt_batch = row
                if nxt_seq != nxt or nxt_batch is not None:
                    break  # gap, or a same-batch group starts here
                end = nxt
            self._apply(CommitGroup(start, end, None), logical_size=end - start + 1)
            committed += 1

    def _apply(self, group: CommitGroup, *, logical_size: int | None = None) -> None:
        self.catalog.apply_range(group.start_seq, group.end_seq, group.batch_id)
        self.counters.groups_committed += 1
        eff_size = logical_size or group.size
        self.counters.largest_group = max(self.counters.largest_group, eff_size)
        self.counters.last_group = group
        if group.batch_id is not None:
            self.counters.batch_groups_committed += 1
        self.counters.history.append(
            {"start_seq": group.start_seq, "end_seq": group.end_seq,
             "size": eff_size, "batch_id": group.batch_id}
        )
        log.info("committed group %s (%d records)",
                 group.batch_id or f"seqs={group.start_seq}..{group.end_seq}", eff_size)
        if self.on_commit is not None:
            self.on_commit(group)
        # Move the just-published prefix toward the cold side. A cold outage
        # leaves the group in hot staging; the next loop's cold_paused() gate
        # bounds how many such groups can accumulate.
        self._try_archive(group)

    def run_once(self) -> SyncCounters:
        """Fetch everything currently available and commit all ready groups."""
        self._pull_and_stage()
        self._commit_ready_groups()
        return self.counters

    def run_forever(self) -> None:
        """Catch-up loop used by the long-running service."""
        log.info("sync loop starting; persisted snapshot at seq %d",
                 self.catalog.last_snapshot_seq())
        next_offset = self._resume_offset()
        while not self._stop:
            try:
                total = self.client.total()
                if self.cold_paused():
                    # Cold side down with one group backlogged: no paging. If
                    # the cold side recovered, a manual archive attempt drains
                    # the backlog; otherwise just retry on the next poll.
                    self.counters.cold_pause_events += 1
                    if self.catalog.cold_available():
                        self._drain_archive_backlog()
                elif self.catalog.last_snapshot_seq() < total:
                    # Only refetch from the first possibly-unstaged page.
                    self._pull_and_stage(start_offset=next_offset)
                    self._commit_ready_groups()
                    # Everything covered up to the fetch watermark is now
                    # known delivered; next poll starts one page after it
                    # (overlap is harmless).
                    next_offset = self._resume_offset()
                else:
                    # Source may grow later; keep the offset at the tail.
                    next_offset = max(0, total - (total % self.page_size))
            except urllib.error.URLError:
                log.warning("log source unreachable, retrying", exc_info=True)
            self._sleep(self.poll_interval)
        log.info("sync loop stopped at snapshot seq %d",
                 self.catalog.last_snapshot_seq())

    def _drain_archive_backlog(self) -> None:
        """After a cold recovery, move every backlogged published group."""
        if self.archiver is None:
            return
        while self.catalog.hot_published_group_count() > 0 and not self._stop:
            try:
                result = self.catalog.archive_published_prefix()
            except ColdUnavailable:
                return
            if result is None:
                return
            self.counters.cold_segments_archived += 1
            if self._cold_was_down:
                self._cold_was_down = False

    def _resume_offset(self) -> int:
        """Lowest page offset that could contain an undelivered seq.

        Uses the durable fetch watermark rather than the max staged seq: after
        archival the hot table may hold no rows at all, while the watermark
        still remembers how far paging had reached. Back off one page so a page
        that failed or arrived partially is re-requested; overlapping
        redelivery is cheap and idempotent (and hot-only on redelivery).
        """
        watermark = max(self.catalog.fetch_watermark(),
                        self.catalog.max_staged_seq())
        if watermark == 0:
            return 0
        raw = max(0, watermark - self.page_size)
        return (raw // self.page_size) * self.page_size

    def run_until_caught_up(self, *, max_passes: int = 200,
                            idle_passes_to_stop: int = 2) -> SyncCounters:
        """Run until snapshot covers the whole source history (demo/tests)."""
        idle = 0
        next_offset = self._resume_offset()
        for _ in range(max_passes):
            if self._stop:
                break
            try:
                total = self.client.total()
            except urllib.error.URLError:
                self._sleep(self.poll_interval)
                continue
            if self.cold_paused():
                # Parked behind a cold outage; keep the loop alive so a
                # recovery within the same process drains the backlog.
                idle = 0
                self.counters.cold_pause_events += 1
                if self.catalog.cold_available():
                    self._drain_archive_backlog()
                self._sleep(self.poll_interval)
                continue
            if self.catalog.last_snapshot_seq() >= total and total > 0:
                idle += 1
                if idle >= idle_passes_to_stop:
                    break
                self._sleep(self.poll_interval)
                continue
            idle = 0
            self._pull_and_stage(start_offset=next_offset)
            self._commit_ready_groups()
            next_offset = self._resume_offset()
            self._sleep(self.poll_interval)
        return self.counters


def make_archiver(catalog: Catalog, target_rows: int = 5000):
    """Build the default on-commit archiver callback for a cold-enabled catalog.

    Each invocation drains every currently eligible slice, but every COPY is
    bounded to ``target_rows`` so the sync thread never blocks on one giant
    export; total archival work stays proportional to history size. A cold
    fault surfaces as ColdUnavailable on the first failing slice and leaves
    the remaining published groups parked in the hot database.
    """

    def _archive(_group: CommitGroup):
        first = catalog.archive_published_prefix(target_rows=target_rows)
        if first is None:
            return None
        while True:
            try:
                piece = catalog.archive_published_prefix(target_rows=target_rows)
            except ColdUnavailable:
                break
            if piece is None:
                break
        return first

    return _archive
