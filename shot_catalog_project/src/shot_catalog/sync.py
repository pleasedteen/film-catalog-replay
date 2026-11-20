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
        # Number of transient 5xx responses retried (delivery was messy).
        self.transient_errors = 0

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
                    self.transient_errors += 1
                    time.sleep(self.backoff * (2 ** attempt))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    self.transient_errors += 1
                    time.sleep(self.backoff * (2 ** attempt))
                    continue
                raise
        raise RuntimeError(f"exhausted retries for {url}: {last_exc}")

    def health(self) -> dict:
        return self._get_json(f"{self.base_url}/health")

    def total(self) -> int:
        """Cheap source-tail probe: one small /health request, no pages."""
        return int(self.health()["total"])

    def fetch_page(self, offset: int, limit: int,
                   films: list[str] | None = None) -> dict:
        params = {"offset": offset, "limit": limit}
        if films is not None:
            # An empty subscription set must ask for *no* film; send a
            # sentinel that matches nothing rather than omitting the filter
            # (omitting it would transfer the whole library).
            params["films"] = ",".join(films) if films else "\x00__none__"
        qs = urllib.parse.urlencode(params)
        return self._get_json(f"{self.base_url}/logs?{qs}")


@dataclass
class SyncCounters:
    pages_fetched: int = 0
    transient_errors: int = 0
    records_seen: int = 0          # including redeliveries
    records_newly_staged: int = 0
    records_transferred: int = 0   # after the sparse film filter (bytes proxy)
    groups_committed: int = 0
    batch_groups_committed: int = 0
    largest_group: int = 0
    last_group: CommitGroup | None = None
    history: list[dict] = field(default_factory=list)
    backfill_pages_fetched: int = 0
    backfill_records_staged: int = 0
    films_activated: int = 0


class SyncEngine:
    def __init__(self, catalog: Catalog, client: LogSourceClient,
                 *, page_size: int = 100, poll_interval: float = 0.15,
                 sleep: Callable[[float], None] = time.sleep,
                 on_commit: Callable[[CommitGroup], None] | None = None,
                 sparse_scan_span: int | None = None):
        self.catalog = catalog
        self.client = client
        self.page_size = page_size
        self.poll_interval = poll_interval
        self._sleep = sleep
        # Invoked after every durable commit group, while holding no engine
        # lock; lets a supervisor observe/act on each prefix advancement.
        self.on_commit = on_commit
        # In sparse mode a request walks this many global seqs at once. Payload
        # stays limited to subscribed films, and the number of requests does
        # not blow up over a sea of unsubscribed revisions.
        self.sparse_scan_span = sparse_scan_span or max(page_size, page_size * 20)
        self._stop = False
        self.counters = SyncCounters()

    def stop(self) -> None:
        self._stop = True

    # ------------------------------------------------------------- one pass
    def _sparse(self) -> bool:
        return self.catalog.has_subscriptions()

    def _scan_limit(self) -> int:
        return self.sparse_scan_span if self._sparse() else self.page_size

    def _pull_and_stage(self, start_offset: int = 0) -> int:
        """Fetch pages from ``start_offset`` to the source tail; return total.

        In sparse mode the request carries the subscribed films, so only their
        shot events are transferred while ``total``/``has_more`` stay global.
        Every page's learned batch extents are persisted so a cross-film batch
        can be published atomically without its unsubscribed remainder.
        """
        sparse = self._sparse()
        limit = self._scan_limit()
        # The forward scan only pulls *active* films; a backfilling film is
        # driven by _backfill_step from its own history, so it is never fetched
        # twice. An empty request is valid -- the global snapshot still moves.
        films = self.catalog.active_films() if sparse else None
        offset = start_offset
        total = None
        while not self._stop:
            try:
                page = self.client.fetch_page(offset, limit, films=films)
            except urllib.error.HTTPError as exc:
                if exc.code >= 500:
                    self.counters.transient_errors += 1
                raise
            self.counters.pages_fetched += 1
            total = page["total"]
            if sparse:
                # Films may have been activated since the pass started.
                films = self.catalog.active_films()
                self.catalog.record_batch_runs(page.get("batch_runs", []))
            raw_records = page["records"]
            normalized: list[NormalizedRecord] = []
            for raw in raw_records:
                try:
                    normalized.append(normalize(raw))
                except LogFormatError:
                    log.exception("malformed record skipped at offset %s", offset)
                    continue
            self.counters.records_seen += len(raw_records)
            self.counters.records_transferred += len(raw_records)
            staged = self.catalog.stage_records(normalized)
            self.counters.records_newly_staged += staged

            # Account for the global window: everything up to the last seq the
            # source exposed is resolved (staged if subscribed, skipped
            # otherwise), so the published snapshot can advance past it.
            if sparse:
                window_end = min(offset + limit, int(total))
                if window_end >= offset + 1 and window_end > 0:
                    self.catalog.mark_resolved(window_end)
            if not page["has_more"]:
                break
            # Overlapping pages are allowed; advance by the scan window
            # regardless of per-film filtering.
            offset += limit
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

        In sparse mode there is no staged row for skipped films, so the group
        walk is driven by the resolved watermark and learned batch extents
        instead of by staging presence.
        """
        if self._sparse():
            self._commit_sparse_groups(ordinary_flush_size)
            return
        while not self._stop:
            group = self.catalog.next_committable_group()
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

    def _commit_sparse_groups(self, ordinary_flush_size: int) -> None:
        while not self._stop:
            group = self.catalog.next_sparse_group()
            if group is None:
                return
            if group.batch_id is not None:
                self._apply(group)
                continue
            start = group.start_seq
            end = min(group.end_seq, start + ordinary_flush_size - 1)
            self._apply(CommitGroup(start, end, None),
                        logical_size=end - start + 1)

    def _apply(self, group: CommitGroup, *, logical_size: int | None = None) -> None:
        if self._sparse():
            self.catalog.apply_publication(group.start_seq, group.end_seq)
        else:
            self.catalog.apply_range(group.start_seq, group.end_seq,
                                     group.batch_id)
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
        self._backfill_step()
        return self.counters

    def run_forever(self) -> None:
        """Catch-up loop used by the long-running service."""
        log.info("sync loop starting; persisted snapshot at seq %d",
                 self.catalog.last_snapshot_seq())
        next_offset = self._resume_offset()
        while not self._stop:
            try:
                watermark = (self.catalog.resolved_seq() if self._sparse()
                             else self.catalog.last_snapshot_seq())
                total = self.client.total()
                if watermark < total:
                    # Only refetch from the first possibly-unstaged page.
                    self._pull_and_stage(start_offset=next_offset)
                    self._commit_ready_groups()
                    # Everything up to the watermark is now accounted for; the
                    # next poll starts one page before its tail (overlap is
                    # harmless and redelivery is idempotent).
                    next_offset = self._resume_offset()
                else:
                    # Source may grow later; keep the offset at the tail.
                    limit = self._scan_limit()
                    next_offset = max(0, total - (total % limit))
                if self._sparse():
                    self._backfill_step()
            except urllib.error.URLError:
                log.warning("log source unreachable, retrying", exc_info=True)
            self.counters.transient_errors = self.client.transient_errors
            self._sleep(self.poll_interval)
        log.info("sync loop stopped at snapshot seq %d",
                 self.catalog.last_snapshot_seq())

    def _resume_offset(self) -> int:
        """Lowest page offset that could contain an unresolved global seq.

        Back off one page from the fetch watermark so a page that failed or
        arrived partially is re-requested; overlapping redelivery is cheap and
        idempotent. In dense mode the watermark is the max staged seq; in
        sparse mode it is the resolved global seq (unsubscribed seqs are never
        staged, so staging alone would understate progress).
        """
        if self._sparse():
            watermark = self.catalog.resolved_seq()
        else:
            watermark = self.catalog.max_staged_seq()
        if watermark == 0:
            return 0
        limit = self._scan_limit()
        raw = max(0, watermark - limit)
        return (raw // limit) * limit

    # ------------------------------------------------------------- backfill
    def _backfill_step(self) -> bool:
        """Advance every 'backfill' film to the published snapshot; activate.

        A newly subscribed film is filled from the start of its staged history
        up to the published snapshot, then flipped to 'active' at that same
        published version. Progress is durable per window, so a restart resumes
        the backfill rather than redoing it, and already-active subscriptions
        are unaffected -- they keep publishing through the normal global path.
        """
        did_work = False
        for film_id in self.catalog.backfill_films():
            if self._stop:
                break
            _, folded = self.catalog.backfill_progress(film_id)
            if folded < self.catalog.last_snapshot_seq():
                self._backfill_walk(film_id, folded)
                did_work = True
            if self.catalog.try_activate_film(film_id):
                self.counters.films_activated += 1
                did_work = True
        return did_work

    def _backfill_walk(self, film_id: str, folded_seq: int) -> None:
        """Page the source for one film and fold its history in order.

        Starts from the window containing ``folded_seq`` and folds only seqs
        strictly greater than it, never beyond the published snapshot. Because
        the folded cursor is the single durable source of truth, a restart
        resumes exactly where folding stopped (re-requesting at most one
        window, which is idempotent) and activation always matches the same
        published version as the already-active subscriptions.
        """
        target = self.catalog.last_snapshot_seq()
        if target <= folded_seq:
            return
        limit = self._scan_limit()
        offset = (folded_seq // limit) * limit
        folded = folded_seq
        while not self._stop:
            try:
                page = self.client.fetch_page(offset, limit, films=[film_id])
            except urllib.error.HTTPError as exc:
                if exc.code >= 500:
                    self.counters.transient_errors += 1
                raise
            self.counters.backfill_pages_fetched += 1
            total = int(page["total"])
            normalized = []
            for raw in page["records"]:
                try:
                    normalized.append(normalize(raw))
                except LogFormatError:
                    continue
            self.counters.backfill_records_staged += \
                self.catalog.stage_records(normalized)
            window_start = offset
            window_end = min(offset + limit, total)
            # Never re-fold a seq already folded, and never fold past the
            # published snapshot (that is the version activation must match).
            fold_from = max(window_start, folded)
            fold_to = min(window_end, target)
            if fold_to > fold_from:
                self.catalog.fold_film_range(film_id, fold_from, fold_to)
                folded = fold_to
                self.catalog.advance_backfill_fetch(film_id, fold_to)
            if window_end >= target or not page["has_more"]:
                break
            offset += limit
            if offset >= total:
                break

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
            done = (self.catalog.last_snapshot_seq() >= total
                    or self.catalog.resolved_seq() >= total)
            if done and total > 0:
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
        # Drain any pending subscriptions that caught up at the very end.
        if self._sparse():
            for _ in range(max_passes):
                if self._stop:
                    break
                self._pull_and_stage(start_offset=0)
                self._commit_ready_groups()
                if not self._backfill_step():
                    break
        self.counters.transient_errors = self.client.transient_errors
        return self.counters
