#!/usr/bin/env python3
"""One-click hot/cold verification over multiple local fixture batches.

Runs entirely on localhost with no external access and no HTTP log source
dependency for the rebuild phase. It proves the cold-archive story end to end:

  1. legacy continuity -- an OLD hot-only database (Catalog with no cold side)
     is synced from a multi-batch fixture, then an independent cold directory
     is attached to the SAME file and all published history is archived;
  2. kill recovery -- a real subprocess worker is SIGKILLed in the middle of
     an archival step (inside the Parquet segment rename); restarting it over
     the same hot + cold directories reconciles tmp/orphan files and converges;
  3. source-off rebuild -- after the log source is shut down, the full history
     is exported to a local JSONL file and a brand-new catalog rebuilds and
     verifies its directory from that file alone;
  4. outage backpressure -- with the cold side offline, online queries keep
     answering, the hot backlog stays at <= one published group and paging
     pauses; recovery drains the backlog automatically.

Run:  python3 -I scripts/demo_cold_archive.py [n_shots]
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT_ROOT / "src"),
                "/workspace/pylibs/lib/python3.11/site-packages"]

from shot_catalog.catalog import Catalog  # noqa: E402
from shot_catalog.cold import ColdStorage  # noqa: E402
from shot_catalog.fixturegen import generate_large_history  # noqa: E402
from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402
from shot_catalog.server import build_handler  # noqa: E402
from shot_catalog.sync import LogSourceClient, SyncEngine, make_archiver  # noqa: E402


def heading(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def _get(base: str, path: str):
    try:
        with urllib.request.urlopen(base + path, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def main() -> int:
    n_shots = int(sys.argv[1]) if len(sys.argv) > 1 else 1200
    work = Path(tempfile.mkdtemp(prefix="shotcat-cold-"))
    history = work / "multi_batch_history.jsonl"
    summary = generate_large_history(history, n_shots=n_shots)
    batch = summary["batch"]
    print(f"workdir            : {work}")
    print(f"fixture events     : {summary['total_events']} "
          f"(same-batch {batch['id']} spans {batch['start_seq']}.."
          f"{batch['end_seq']})")

    source = PagedLogSource(history, page_size_hint=200, shuffle=True,
                            duplicate_rate=0.35, error_rate=0.1, seed=20261120)
    httpd = serve(source, "127.0.0.1", 0)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    worker = PROJECT_ROOT / "scripts" / "_cold_worker.py"

    # ------------------------------------------------------- 1. legacy database
    heading("1. Legacy hot-only database, then attach an independent cold dir")
    legacy_db = work / "legacy.duckdb"
    with Catalog(legacy_db) as cat:
        eng = SyncEngine(cat, LogSourceClient(url), page_size=200,
                         poll_interval=0.0)
        eng.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)
        assert cat.last_snapshot_seq() == summary["total_events"]
        assert cat.stats()["cold_enabled"] is False
        print(f"hot-only sync complete: snapshot_seq={cat.last_snapshot_seq()} "
              f"visible={cat.stats()['visible_shots']}")

    cold_dir = work / "cold"
    with Catalog(legacy_db, cold=ColdStorage(cold_dir)) as cat:
        results = cat.drain_hot_to_cold(target_rows=400)
        assert cat.archived_through_seq() == summary["total_events"]
        assert cat.verify_full_history_oracle() == 0
        print(f"attached cold dir, archived {len(results)} segments, "
              f"{cat.stats()['cold_events']} events; oracle mismatch = "
              f"{cat.verify_full_history_oracle()}")

    # ------------------------------------------------- 2. SIGKILL mid-archival
    heading("2. SIGKILL a worker mid-archival, then restart and converge")
    kill_db = work / "killed.duckdb"
    kill_cold = work / "killed_cold"
    progress = work / "progress.txt"
    env = dict(os.environ)
    env["PYTHONPATH"] = (f"{PROJECT_ROOT / 'src'}:"
                         "/workspace/pylibs/lib/python3.11/site-packages")
    env["COLD_DIR"] = str(kill_cold)
    env["ARCHIVE_ROWS"] = "300"
    env["KILL_COLD_OP"] = "segment_rename"
    env["PROGRESS_FILE"] = str(progress)
    proc = subprocess.Popen(
        [sys.executable, "-I", str(worker), str(kill_db), url, "200", "60"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        if progress.exists() and progress.read_text().strip():
            break
        if proc.poll() is not None:
            time.sleep(0.2)
            break
        time.sleep(0.05)
    proc.wait(timeout=10)
    assert proc.returncode != 0, "worker was supposed to be SIGKILLed"
    print(f"worker killed (rc={proc.returncode}); tmp leftovers before "
          f"restart: {len(list((kill_cold / 'segments').glob('*.tmp')))}")

    resume_env = {k: v for k, v in env.items()
                  if k not in ("KILL_COLD_OP", "PROGRESS_FILE")}
    proc2 = subprocess.run(
        [sys.executable, "-I", str(worker), str(kill_db), url, "200", "0"],
        env=resume_env, capture_output=True, text=True, timeout=120)
    assert proc2.returncode == 0, proc2.stderr
    print(f"restart converged: {proc2.stdout.strip()}")
    with Catalog(kill_db, cold=ColdStorage(kill_cold)) as cat:
        assert cat.last_snapshot_seq() == summary["total_events"]
        assert cat.verify_full_history_oracle() == 0
        leftovers = list((kill_cold / "segments").glob("*.tmp"))
        assert leftovers == []
        print(f"reconciled: {cat.stats()['cold_segments']} segments, "
              f"zero tmp leftovers, oracle mismatch = 0")

    # --------------------------------------------- 3. outage backpressure + HTTP
    heading("3. Cold outage: queries survive, backlog <= 1 group, paging pauses")
    outage_db = work / "outage.duckdb"
    outage_cold = ColdStorage(work / "outage_cold")
    with Catalog(outage_db, cold=outage_cold) as cat:
        eng = SyncEngine(cat, LogSourceClient(url), page_size=200,
                         poll_interval=0.0,
                         archiver=make_archiver(cat, target_rows=300))
        eng._pull_and_stage()
        eng._commit_ready_groups(max_groups=1)
        http_query = ThreadingHTTPServer(
            ("127.0.0.1", 0), build_handler(cat))
        threading.Thread(target=http_query.serve_forever, daemon=True).start()
        qbase = f"http://127.0.0.1:{http_query.server_address[1]}"
        try:
            outage_cold.set_offline()
            eng.run_until_caught_up(max_passes=8, idle_passes_to_stop=1)
            assert cat.hot_published_group_count() <= 1
            code, health = _get(qbase, "/health")
            assert code == 200 and health["cold_available"] is False
            code, status = _get(qbase, "/cold/status")
            assert code == 200 and status["hot_published_backlog_groups"] <= 1
            print(f"offline: /health 200 (cold_available=false), backlog "
                  f"groups={status['hot_published_backlog_groups']}, "
                  f"pause events={eng.counters.cold_pause_events}")

            outage_cold.set_online()
            eng.run_until_caught_up(max_passes=60, idle_passes_to_stop=1)
            cat.drain_hot_to_cold(target_rows=300)
            assert cat.last_snapshot_seq() == summary["total_events"]
            assert cat.hot_published_group_count() == 0
            assert cat.verify_full_history_oracle() == 0
            code, health = _get(qbase, "/health")
            assert code == 200 and health["cold_available"] is True
            print("recovered: backlog drained automatically, full catch-up "
                  "and verification OK")
        finally:
            http_query.shutdown()

    # ------------------------------------------------- 4. source-off rebuild
    heading("4. Shut the log source down; rebuild from local history only")
    local_jsonl = work / "local_history.jsonl"
    with Catalog(legacy_db, cold=ColdStorage(cold_dir)) as cat:
        exported = cat.export_history_jsonl(local_jsonl)
        expected = cat.list_film_shots("film-bulk")
    httpd.shutdown()  # the HTTP log source is gone from here on

    rebuilt_db = work / "rebuilt.duckdb"
    with Catalog(rebuilt_db, cold=ColdStorage(work / "rebuilt_cold")) as rebuilt:
        loaded = rebuilt.load_jsonl_history(local_jsonl)
        assert loaded == exported == summary["total_events"]

        class _LocalOnly:
            def total(self):
                return summary["total_events"]

        eng = SyncEngine(rebuilt, _LocalOnly(), page_size=200,
                         poll_interval=0.0)
        eng._commit_ready_groups()
        assert rebuilt.last_snapshot_seq() == summary["total_events"]
        assert rebuilt.rebuild_mismatch_count() == 0
        got = rebuilt.list_film_shots("film-bulk")
        assert got["shots"] == expected["shots"]
        print(f"rebuilt {loaded} events from local JSONL with no HTTP source; "
              f"film-bulk shots match ({got['count']})")

    print("\nCOLD ARCHIVE DEMO OK")
    print(f"artifacts kept in: {work}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
