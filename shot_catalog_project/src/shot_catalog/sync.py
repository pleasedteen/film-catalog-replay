"""Paged catch-up synchronization engine.

The engine never trusts delivery order. It repeatedly pulls pages from the
local HTTP log source, normalizes and idempotently merges records into DuckDB
staging, then commits whatever prefix groups are now complete. Memory holds
at most one page at a time -- even a 100k-shot revision spanning thousands of
pages is never assembled in Python.

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


class SyncEngine:
    def __init__(self, catalog: Catalog, client: LogSourceClient,
                 *, page_size: int = 100, poll_interval: float = 0.15,
                 sleep: Callable[[float], None] = time.sleep,
                 on_commit: Callable[[CommitGroup], None] | None = None,
                 before_commit: Callable[[CommitGroup], None] | None = None):
        self.catalog = catalog
        self.client = client
        self.page_size = page_size
        self.poll_interval = poll_interval
        self._sleep = sleep
        # Invoked after every durable commit group, while holding no engine
        # lock; lets a supervisor observe/act on each prefix advancement.
        self.on_commit = on_commit
        # Invoked for every commit group *inside* its still-uncommitted
        # transaction (before publication). Tests/demos use it to freeze a
        # cross-page revision at the moment that used to block readers.
        self.before_commit = before_commit
        self._stop = False
        self.counters = SyncCounters()

    def stop(self) -> None:
        self._stop = True

    # ------------------------------------------------------------- one pass
    def _pull_and_stage(self, start_offset: int = 0) -> int:
        """Fetch pages from ``start_offset`` to the source tail; return total."""
        offset = start_offset
        total = None
        while not self._stop:
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
            if not page["has_more"]:
                break
            # Overlapping pages are allowed; advance by page_size regardless.
            offset += self.page_size
            if offset >= total:
                break
        return total or 0

    def _commit_ready_groups(self, ordinary_flush_size: int = 5000) -> None:
        """Commit every prefix group that is fully staged.

        Same-batch revisions are always applied as their own atomic group.
        Consecutive ordinary records are coalesced into one range apply per
        ``ordinary_flush_size`` records so a 100k-event prefix costs a few
        hundred transactions instead of 100k -- while still advancing the
        durable snapshot only through complete, contiguous history.
        """
        while not self._stop:
            group = self.catalog.next_committable_group()
            if group is None:
                return
            if group.batch_id is not None:
                self._apply(group)
                continue
            # Coalesce ready ordinary records: the flush ends just before a
            # batch boundary or at the first seq that has not been durably
            # staged yet. ``ordinary_run_end`` resolves that boundary inside
            # DuckDB (one statement) instead of walking seq by seq in Python.
            start = group.start_seq
            limit = start + ordinary_flush_size - 1
            end = self.catalog.ordinary_run_end(start, limit)
            self._apply(CommitGroup(start, end, None), logical_size=end - start + 1)

    def _apply(self, group: CommitGroup, *, logical_size: int | None = None) -> None:
        # ``before_commit`` is the seam used by tests/demos to park a
        # cross-page revision immediately before publication; it must not
        # block readers (they hold their own snapshots) nor be skipped.
        hook = (lambda: self.before_commit(group)) if self.before_commit else None
        self.catalog.apply_range(group.start_seq, group.end_seq, group.batch_id,
                                 before_commit=hook)
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
                if self.catalog.last_snapshot_seq() < total:
                    # Only refetch from the first possibly-unstaged page.
                    self._pull_and_stage(start_offset=next_offset)
                    self._commit_ready_groups()
                    # Everything covered up to max_staged_seq is now fetchable;
                    # next poll starts one page after it (overlap is harmless).
                    next_offset = self._resume_offset()
                else:
                    # Source may grow later; keep the offset at the tail.
                    next_offset = max(0, total - (total % self.page_size))
            except urllib.error.URLError:
                log.warning("log source unreachable, retrying", exc_info=True)
            self._sleep(self.poll_interval)
        log.info("sync loop stopped at snapshot seq %d",
                 self.catalog.last_snapshot_seq())

    def _resume_offset(self) -> int:
        """Lowest page offset that could contain an unstaged seq.

        Back off one page from the max staged seq so a page that failed or
        arrived partially is re-requested; overlapping redelivery is cheap and
        idempotent.
        """
        max_staged = self.catalog.max_staged_seq()
        if max_staged == 0:
            return 0
        raw = max(0, max_staged - self.page_size)
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
