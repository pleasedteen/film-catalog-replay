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
                 on_commit: Callable[[CommitGroup], None] | None = None):
        self.catalog = catalog
        self.client = client
        self.page_size = page_size
        self.poll_interval = poll_interval
        self._sleep = sleep
        # Invoked after every durable commit group, while holding no engine
        # lock; lets a supervisor observe/act on each prefix advancement.
        self.on_commit = on_commit
        self._stop = False
        self.counters = SyncCounters()

    def stop(self) -> None:
        self._stop = True

    # ------------------------------------------------------------- one pass
    def _pull_and_stage(self, start_offset: int = 0,
                        total: int | None = None) -> int:
        """Fetch pages from ``start_offset`` to the source tail; return total.

        Pages whose whole seq window is already durably staged are skipped
        without a request, so after a restart only missing pages are
        re-pulled: a completely staged page is never fetched again. Fetching
        always resumes at the first *unstaged* page (see
        :meth:`_resume_offset`), so pages that arrived before a missing page
        cannot make the engine jump past the gap.
        """
        offset = start_offset
        if total is None:
            total = self.client.total()
        reached_tail = False
        while not self._stop:
            page_end = min(offset + self.page_size - 1, total)
            if self.catalog.is_range_fully_staged(offset + 1, page_end):
                log.info("skip fully-staged page offset=%d seqs=%d..%d",
                         offset, offset + 1, page_end)
            else:
                try:
                    page = self.client.fetch_page(offset, self.page_size)
                except urllib.error.HTTPError as exc:
                    if exc.code >= 500:
                        # Retries inside the client are exhausted; leave the
                        # page un-staged and end this pass. The page keeps its
                        # place as the first-unstaged page, so the next pass
                        # re-requests exactly it instead of jumping past it.
                        self.counters.transient_errors += 1
                        log.warning("page offset=%d unavailable after retries; "
                                    "will retry next pass", offset)
                        return total or 0
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
                log.info("fetched page offset=%d seqs=%d..%d new_staged=%d",
                         offset, offset + 1, page_end, staged)
                reached_tail = not page["has_more"]
            if reached_tail or offset + self.page_size >= total:
                break
            # Overlapping pages are allowed; advance by page_size regardless.
            offset += self.page_size
        return total or 0

    def _commit_ready_groups(self, ordinary_flush_size: int = 5000,
                             source_total: int | None = None) -> None:
        """Commit every prefix group that is fully staged.

        Same-batch revisions are always applied as their own atomic group.
        Consecutive ordinary records are coalesced into one range apply per
        ``ordinary_flush_size`` records so a 100k-event prefix costs a few
        hundred transactions instead of 100k -- while still advancing the
        durable snapshot only through complete, contiguous history.
        """
        while not self._stop:
            group = self.catalog.next_committable_group(source_total)
            if group is None:
                return
            if group.batch_id is not None:
                self._apply(group)
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

    def run_once(self) -> SyncCounters:
        """Fetch everything currently available and commit all ready groups."""
        total = self._pull_and_stage()
        self._commit_ready_groups(source_total=total)
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
                    # Only refetch from the first page containing an unstaged
                    # seq; fully staged pages inside the window are not
                    # requested again.
                    total = self._pull_and_stage(start_offset=next_offset,
                                                 total=total)
                    self._commit_ready_groups(source_total=total)
                    next_offset = self._resume_offset(total)
                else:
                    # Caught up; park at the tail page boundary. When the
                    # source grows, _resume_offset points at the first new
                    # (possibly partially filled) page again.
                    next_offset = max(0, total - (total % self.page_size))
            except urllib.error.URLError:
                log.warning("log source unreachable, retrying", exc_info=True)
            self._sleep(self.poll_interval)
        log.info("sync loop stopped at snapshot seq %d",
                 self.catalog.last_snapshot_seq())

    def _resume_offset(self, total: int | None = None) -> int:
        """Page offset that contains the first unstaged seq.

        The resume point is derived from the *first missing seq* of the
        prefix window rather than from ``max(staged seq)``: when later pages
        arrive before an earlier one (or an earlier page was lost across a
        restart), the engine must go back to the hole and backfill it before
        anything beyond the hole can publish. When nothing is missing the
        result lands exactly on the tail page boundary, so completely staged
        pages are never re-requested.
        """
        if total is None:
            total = self.client.total()
        first_missing = self.catalog.first_unstaged_seq(
            self.catalog.last_snapshot_seq(), total
        )
        if first_missing > total:
            # Everything up to the known source tail is staged: park on the
            # last page boundary (a zero-width offset when tail == page size).
            return max(0, total - (total % self.page_size))
        return ((first_missing - 1) // self.page_size) * self.page_size

    def run_until_caught_up(self, *, max_passes: int = 200,
                            idle_passes_to_stop: int = 2) -> SyncCounters:
        """Run until snapshot covers the whole source history (demo/tests)."""
        idle = 0
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
            next_offset = self._resume_offset(total)
            total = self._pull_and_stage(start_offset=next_offset, total=total)
            self._commit_ready_groups(source_total=total)
            self._sleep(self.poll_interval)
        return self.counters
