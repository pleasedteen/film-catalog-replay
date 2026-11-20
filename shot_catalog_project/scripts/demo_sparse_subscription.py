#!/usr/bin/env python3
"""Sparse-subscription demonstration.

Proves, on localhost with bundled fixtures and no external access:

  1. Sparse transfer: the warehouse subscribes to two films in a library whose
     bulk is an unsubscribed film with thousands of ordinary revisions. Only
     the subscribed films' shot events cross the wire, so transferred volume
     stays flat while the unsubscribed churn grows.
  2. Global snapshot: the published snapshot advances across the whole library,
     including the unsubscribed span.
  3. Cross-film batch: a same-batch revision touching a subscribed shot and an
     unsubscribed shot publishes the subscribed survivor as one atomic group,
     without transferring the unsubscribed remainder.
  4. Append + backfill: a late film is subscribed; it is hidden while it
     backfills, then becomes queryable at the same published version as the
     existing subscriptions, without disturbing them.
  5. Interrupted backfill: progress is durable; a fresh Catalog over the same
     file resumes the backfill instead of restarting it.

Run:  python3 -I scripts/demo_sparse_subscription.py
"""

import sys
import tempfile
import threading
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"),
                "/workspace/pylibs/lib/python3.11/site-packages"]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.fixturegen import generate_sparse_history  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine  # noqa: E402


def heading(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def _serve(path):
    source = PagedLogSource(path, page_size_hint=100, shuffle=True,
                            duplicate_rate=0.3, error_rate=0.1, seed=20261120)
    httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return source, httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _transferred(n_skip, workdir):
    hist = generate_sparse_history(workdir / f"h{n_skip}.jsonl",
                                   n_unsubscribed=n_skip, batch_across=12)
    source, httpd, url = _serve(hist["path"])
    try:
        with Catalog(workdir / f"c{n_skip}.duckdb") as cat:
            cat.add_subscription("film-sub-a")
            cat.add_subscription("film-sub-b")
            eng = SyncEngine(cat, LogSourceClient(url), page_size=100,
                             poll_interval=0.0)
            eng.run_until_caught_up(max_passes=120, idle_passes_to_stop=1)
            return (eng.counters.records_transferred, hist["total_events"],
                    cat.last_snapshot_seq(), cat.rebuild_mismatch_count())
    finally:
        httpd.shutdown()


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="shotcat-sparse-"))
    print(f"workdir: {workdir}")

    heading("1. Sparse transfer stays flat as the unsubscribed library grows")
    small_xfer, small_total, _, _ = _transferred(2000, workdir)
    big_xfer, big_total, _, _ = _transferred(8000, workdir)
    print(f"library {small_total:>6} events -> transferred {small_xfer:>3} records")
    print(f"library {big_total:>6} events -> transferred {big_xfer:>3} records")
    print(f"4x more unsubscribed churn, transferred delta = "
          f"{big_xfer - small_xfer} (must be 0)")
    assert big_xfer == small_xfer
    assert big_total > 3 * small_total

    hist = generate_sparse_history(workdir / "sparse.jsonl",
                                   n_unsubscribed=4000, batch_across=12)
    source, httpd, url = _serve(hist["path"])
    db = workdir / "sparse.duckdb"
    try:
        heading("2. Global snapshot advances past unsubscribed history")
        with Catalog(db) as cat:
            cat.add_subscription("film-sub-a")
            cat.add_subscription("film-sub-b")
            eng = SyncEngine(cat, LogSourceClient(url), page_size=100,
                             poll_interval=0.0)
            eng.run_until_caught_up(max_passes=120, idle_passes_to_stop=1)
            stats = cat.stats()
            print(f"snapshot_seq = {cat.last_snapshot_seq()} / "
                  f"source total = {hist['total_events']}")
            print(f"staged events (subscribed only) = "
                  f"{stats['staged_events']} of {hist['total_events']}")
            print(f"unsubscribed film queryable? "
                  f"{cat.get_shot('film-skip', 'SK001') is not None}")
            assert cat.last_snapshot_seq() == hist["total_events"]
            assert cat.get_shot("film-skip", "SK001") is None
            assert cat.rebuild_mismatch_count() == 0

            heading("3. Cross-film batch publishes subscribed survivor atomically")
            a001 = cat.get_shot("film-sub-a", "A001")
            print(f"batch {hist['batch']['id']} spans "
                  f"{hist['batch']['start_seq']}..{hist['batch']['end_seq']} "
                  f"(subscribed + unsubscribed shots)")
            print(f"A001 after batch: duration_ms={a001['duration_ms']} "
                  f"repair_note={a001['repair_note']!r}")
            assert a001["duration_ms"] == 1250
            assert a001["repair_note"] == "cross-batch"

            heading("4. Append subscription backfills only the new film")
            snap_before = cat.last_snapshot_seq()
            status = cat.add_subscription("film-late")
            print(f"subscribe film-late -> status {status!r} "
                  f"(hidden until caught up)")
            assert cat.get_shot("film-late", "L001") is None
            eng.run_until_caught_up(max_passes=120, idle_passes_to_stop=1)
            late = cat.get_shot("film-late", "L001")
            print(f"after backfill: film-late status="
                  f"{cat.subscription_status('film-late')!r} "
                  f"L001 duration_ms={late['duration_ms']}")
            print(f"snapshot still >= {snap_before}: "
                  f"{cat.last_snapshot_seq()} ; old subscription unchanged: "
                  f"{cat.get_shot('film-sub-a', 'A001') == a001}")
            assert late["duration_ms"] == 800
            assert cat.subscription_status("film-late") == "active"
            assert cat.get_shot("film-sub-a", "A001") == a001

        heading("5. Interrupted backfill resumes from durable progress")
        # Re-subscribe a fresh film and fold only part of its history, then
        # "crash" before activation: exactly the durable partial progress a
        # SIGKILL mid-backfill would leave behind.
        late_start = hist["late"]["start_seq"]
        with Catalog(db) as cat:
            cat._conn.execute(
                "UPDATE film_subscriptions SET status = 'backfill', "
                "backfill_fetch_seq = 0, backfilled_seq = 0, activated_at = NULL "
                "WHERE film_id = 'film-late'"
            )
            from shot_catalog.log_format import normalize
            raw = LogSourceClient(url).fetch_page(0, 10000,
                                                  films=["film-late"])["records"]
            cat.stage_records([normalize(r) for r in raw])
            cat.fold_film_range("film-late", 0, late_start)
            cat.advance_backfill_fetch("film-late", late_start)
        # Fresh Catalog over the same file sees the durable partial progress.
        with Catalog(db) as cat2:
            fetched, folded = cat2.backfill_progress("film-late")
            print(f"reopened; film-late status="
                  f"{cat2.subscription_status('film-late')!r} "
                  f"progress=(fetch={fetched}, folded={folded}) "
                  f"-> resumes folding seqs >{folded}")
            assert folded == late_start > 0
            assert cat2.subscription_status("film-late") == "backfill"
            assert cat2.get_shot("film-late", "L001") is None  # still hidden
            eng2 = SyncEngine(cat2, LogSourceClient(url), page_size=100,
                              poll_interval=0.0)
            eng2.run_until_caught_up(max_passes=120, idle_passes_to_stop=1)
            print(f"resumed and finished: status="
                  f"{cat2.subscription_status('film-late')!r} "
                  f"L002 duration_ms="
                  f"{cat2.get_shot('film-late', 'L002')['duration_ms']}")
            print(f"rebuild-oracle mismatch = {cat2.rebuild_mismatch_count()}")
            assert cat2.subscription_status("film-late") == "active"
            assert cat2.rebuild_mismatch_count() == 0
    finally:
        httpd.shutdown()

    heading("SPARSE SUBSCRIPTION DEMO OK")
    print(f"artifacts kept in: {workdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
