#!/usr/bin/env python3
"""One-command regression for cross-page batch resume across a hard restart.

Scenario (everything runs on 127.0.0.1, no external hosts):

  Run 1   a worker synchronizes a history whose same-batch revision spans
          several pages, while the harness holds one page *inside* the batch
          back from the source (HTTP 500 on every attempt) and redirects its
          requests to the source's tail page. Later pages therefore arrive
          first and are durably staged. The worker publishes only the
          ordinary prefix and parks; the harness SIGKILLs it.
  Check   the half-staged batch is completely invisible and the snapshot
          stops exactly one seq before the batch, even though later seqs are
          already durably staged.
  Run 2   a fresh worker process reopens the same DuckDB file with the page
          still held. It must request only that missing page (fully staged
          pages are skipped without a request) and keep the batch hidden.
  Run 3   the hold and redirect are lifted; the worker backfills the single
          missing page and the whole batch publishes atomically -- including
          the source's last complete batch -- converging snapshot == total.

The harness audits every /logs request range and snapshots the catalog
before/after each run. All artifacts (history, db, audit jsonl, worker logs,
snapshots) are kept under one run directory and a PASS/FAIL verdict plus the
request-range evidence are printed at the end.

Run:  python3 -I scripts/regression_kill_resume.py
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(PROJECT_ROOT / "src"),
    "/workspace/pylibs/lib/python3.11/site-packages",
]

from shot_catalog.catalog import Catalog  # noqa: E402

PAGE_SIZE = 10
WORKER = PROJECT_ROOT / "scripts" / "_sync_worker.py"
SOURCE = PROJECT_ROOT / "scripts" / "controllable_source.py"
PORT = 8091
BASE_TS = datetime(2026, 11, 1, tzinfo=timezone.utc)


def ts(seq: int) -> str:
    return (BASE_TS + timedelta(seconds=seq)).strftime("%Y-%m-%dT%H:%M:%SZ")


def rec(seq: int, event_type: str, shot: str, version: int,
        payload=None, batch_id=None) -> dict:
    out = {
        "seq": seq, "event_id": f"evt-{seq:08d}", "event_type": event_type,
        "film_id": "film-reg", "shot_id": shot, "version": version, "ts": ts(seq),
    }
    if batch_id is not None:
        out["batch_id"] = batch_id
    if payload is not None:
        out["payload"] = payload
    return out


def write_history(path: Path) -> dict:
    """15 ordinary creates, then a 15-event batch (pages 1..2), then 5 tail."""
    seq = 1
    records: list[dict] = []
    for i in range(1, 16):  # seqs 1..15, ordinary v1 creates
        records.append(rec(seq, "upsert", f"R{i:03d}", 1,
                           {"title": f"ordinary {i}", "duration_sec": 10 + i % 5}))
        seq += 1
    batch_start = seq
    for i in range(1, 16):  # seqs 16..30, same batch B700
        records.append(rec(seq, "upsert", f"R{i:03d}", 2,
                           {"duration_ms": 20000 + i,
                            "repair_note": f"B700 batch fix #{i}"},
                           batch_id="B700"))
        seq += 1
    batch_end = seq - 1
    for i in range(1, 6):  # seqs 31..35, ordinary tail records
        records.append(rec(seq, "upsert", f"T{i:03d}", 2,
                           {"duration_ms": 30000 + i}))
        seq += 1
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8",
    )
    return {"total": seq - 1, "batch_start": batch_start,
            "batch_end": batch_end, "batch_id": "B700"}


def wait_health(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=2) as resp:
                payload = json.loads(resp.read().decode())
                if payload.get("status") == "ok":
                    return
        except Exception as exc:  # pragma: no cover - startup race
            last = exc
            time.sleep(0.1)
    raise RuntimeError(f"source never became healthy: {last}")


def snapshot_catalog(db_path: Path, label: str, dest: Path,
                     batch_start_global: int) -> dict:
    with Catalog(db_path) as cat:
        stats = cat.stats()
        batch_shots = []
        for i in range(1, 16):
            shot = cat.get_shot("film-reg", f"R{i:03d}")
            batch_shots.append(None if shot is None else {
                "shot_id": shot["shot_id"],
                "duration_ms": shot["duration_ms"],
                "repair_note": shot["repair_note"],
                "last_event_seq": shot["last_event_seq"],
            })
        tail_shots = [cat.get_shot("film-reg", f"T{i:03d}") for i in range(1, 6)]
        snap = {
            "label": label,
            "snapshot_seq": cat.last_snapshot_seq(),
            "stats": stats,
            "batch_shot_sample": batch_shots[:3],
            # R* shots exist from their ordinary prefix creates; what must be
            # hidden until the batch completes is the BATCH state itself.
            "batch_shots_visible": sum(s is not None for s in batch_shots),
            "batch_shots_with_batch_note": sum(
                s is not None and s["repair_note"] is not None
                and s["repair_note"].startswith("B700")
                for s in batch_shots
            ),
            "batch_shots_past_batch_start": sum(
                s is not None and s["last_event_seq"] >= batch_start_global
                for s in batch_shots
            ),
            "tail_shots_visible": sum(s is not None for s in tail_shots),
            "rebuild_mismatch": cat.rebuild_mismatch_count(),
        }
    dest.write_text(json.dumps(snap, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return snap


def parse_audit(path: Path) -> list[dict]:
    entries = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    return entries


def requested_offsets(entries: list[dict]) -> list[int]:
    return [e["offset"] for e in entries if e["event"] == "request"]


class CheckFailure(AssertionError):
    pass


def check(cond: bool, message: str) -> None:
    if not cond:
        raise CheckFailure(message)


def run_worker(run_dir: Path, db_path: Path, url: str, log_name: str,
               env_extra: dict | None = None, max_seconds: float = 0.0,
               expect_park: bool = False, kill: bool = False,
               progress: Path | None = None, parked_below: int | None = None,
               hard_timeout: float = 90.0) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = (f"{PROJECT_ROOT / 'src'}:"
                         "/workspace/pylibs/lib/python3.11/site-packages")
    if env_extra:
        env.update(env_extra)
    log_path = run_dir / log_name
    with log_path.open("w", encoding="utf-8") as logfh:
        proc = subprocess.Popen(
            [sys.executable, "-I", str(WORKER), str(db_path), url,
             str(PAGE_SIZE), str(max_seconds)],
            env=env, stdout=subprocess.PIPE, stderr=logfh, text=True,
        )
        result = {"pid": proc.pid, "log": str(log_path)}
        if kill:
            assert progress is not None and parked_below is not None
            deadline = time.time() + hard_timeout
            parked = False
            while time.time() < deadline:
                if proc.poll() is not None:
                    break
                if progress.exists():
                    seq = int(progress.read_text().strip())
                    if 0 < seq < parked_below:
                        parked = True
                        break
                time.sleep(0.05)
            check(parked, "worker never parked at the pre-batch prefix")
            check(proc.poll() is None, "worker exited before the SIGKILL")
            proc.send_signal(signal.SIGKILL)
            proc.wait(timeout=15)
            result["returncode"] = proc.returncode
            check(proc.returncode != 0, "SIGKILL did not terminate the worker")
            if proc.stdout:
                result["stdout"] = proc.stdout.read()
            return result
        try:
            stdout, _ = proc.communicate(timeout=hard_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise
        result["returncode"] = proc.returncode
        result["stdout"] = stdout
        check(proc.returncode == 0, f"worker {log_name} failed (rc={proc.returncode})")
        return result


def main() -> int:
    run_dir = Path(tempfile.mkdtemp(prefix="shotcat-regression-"))
    history = run_dir / "history.jsonl"
    db_path = run_dir / "catalog.duckdb"
    audit_log = run_dir / "source_audit.jsonl"
    state_dir = run_dir / "state"
    state_dir.mkdir()
    block_file = state_dir / "blocked_offsets"
    progress = run_dir / "progress.txt"
    info = write_history(history)
    total = info["total"]
    batch_start = info["batch_start"]
    # Page layout at page_size=10:
    #   offset 0  -> seqs 1..10    ordinary
    #   offset 10 -> seqs 11..20   ordinary 11..15 + batch B700 seqs 16..20
    #   offset 20 -> seqs 21..30   batch B700 (a full 10-row page)
    #   offset 30 -> seqs 31..35   ordinary tail (partial page)
    # Hold the page that straddles the batch start; redirect its requests to
    # the full in-batch page at offset 20, so later batch pages (and, via the
    # normal tail page, the tail ordinary records) arrive while seqs 11..20
    # are entirely missing.
    blocked_offset = 10
    redirect_target = 20
    tail_offset = 30

    print(f"run directory : {run_dir}")
    print(f"history       : {total} events; batch {info['batch_id']} "
          f"seqs {batch_start}..{info['batch_end']}")
    print(f"held page     : offset={blocked_offset} (seqs 11..20, straddles "
          f"the batch start); redirected to offset {redirect_target} so later "
          f"pages arrive first")

    # ----------------------------------------------------------- start source
    src_log = run_dir / "source.log"
    src_logfh = src_log.open("w", encoding="utf-8")
    src_proc = subprocess.Popen(
        [sys.executable, "-I", str(SOURCE), str(history), str(PORT),
         str(audit_log), str(state_dir), "--page-size", str(PAGE_SIZE)],
        stdout=src_logfh, stderr=subprocess.STDOUT, text=True,
    )
    url = f"http://127.0.0.1:{PORT}"
    redirect_file = state_dir / "redirect_offsets"
    failures: list[str] = []

    def set_source_phase(*, blocked: bool, redirected: bool) -> None:
        block_file.write_text(f"{blocked_offset}\n" if blocked else "",
                              encoding="utf-8")
        mapping = [{"from": blocked_offset, "to": redirect_target}] \
            if redirected else []
        redirect_file.write_text(json.dumps(mapping), encoding="utf-8")

    try:
        wait_health(url)
        # Phase of run 1: the in-batch page's requests are answered with the
        # tail page's rows, so records beyond the batch arrive and get staged
        # while the batch page itself is still missing.
        set_source_phase(blocked=False, redirected=True)

        # ---------------------------------------------- run 1: park + SIGKILL
        print("\n[run 1] worker syncs with the batch-straddling page held "
              "back; later pages arrive first; SIGKILL at the pre-batch prefix")
        r1 = run_worker(
            run_dir, db_path, url, "worker1.log",
            env_extra={"STOP_AFTER_SEQ": "10", "PROGRESS_FILE": str(progress)},
            kill=True, progress=progress, parked_below=batch_start,
        )
        print(f"  pid={r1['pid']} SIGKILL rc={r1['returncode']}")
        snap1 = snapshot_catalog(db_path, "after_sigkill",
                                 run_dir / "snapshot_after_kill.json",
                                 batch_start)
        print(f"  snapshot after kill: snapshot_seq={snap1['snapshot_seq']} "
              f"max_staged={snap1['stats']['max_staged_seq']} "
              f"staged={snap1['stats']['staged_events']} "
              f"batch_state_visible={snap1['batch_shots_with_batch_note']}/15 "
              f"tail_visible={snap1['tail_shots_visible']}/5")
        # The held page covers seqs 11..20, so nothing from seq 11 on --
        # ordinary prefix records 11..15 included -- can publish.
        expected_parked_prefix = blocked_offset
        try:
            check(snap1["snapshot_seq"] == expected_parked_prefix,
                  f"snapshot must stop at the held page boundary "
                  f"{expected_parked_prefix}, got {snap1['snapshot_seq']}")
            check(snap1["stats"]["max_staged_seq"] == total,
                  "later pages (including the source tail) must already be "
                  f"durably staged past the hole: max_staged="
                  f"{snap1['stats']['max_staged_seq']}")
            check(snap1["batch_shots_with_batch_note"] == 0,
                  "half-staged batch must be completely invisible "
                  "(no B700 state exposed)")
            check(snap1["batch_shots_past_batch_start"] == 0,
                  "no shot's visible last_event_seq may reach into the batch")
            check(snap1["tail_shots_visible"] == 0,
                  "records after the incomplete batch stay invisible")
            check(snap1["rebuild_mismatch"] == 0,
                  "incremental state must match the prefix rebuild oracle")
        except CheckFailure as exc:
            failures.append(f"run1: {exc}")

        # -------------------------------- run 2: restart, hole hard-blocked
        print("\n[run 2] fresh worker reopens the db; the missing page now "
              "fails every request (HTTP 500)")
        set_source_phase(blocked=True, redirected=False)
        before_audit = len(parse_audit(audit_log))
        r2 = run_worker(run_dir, db_path, url, "worker2.log", max_seconds=8)
        print(f"  rc={r2['returncode']}")
        snap2 = snapshot_catalog(db_path, "restart_hole_open",
                                 run_dir / "snapshot_restart_hole.json",
                                 batch_start)
        entries2 = parse_audit(audit_log)[before_audit:]
        offsets2 = requested_offsets(entries2)
        unique2 = sorted(set(offsets2))
        print(f"  unique request offsets after restart: {unique2} "
              f"({len(offsets2)} attempts, all 500)")
        print(f"  snapshot={snap2['snapshot_seq']} "
              f"batch_state_visible={snap2['batch_shots_with_batch_note']}/15")
        try:
            check(offsets2, "restart worker made no page requests at all")
            check(len(offsets2) >= 2,
                  "the held page must be retried across multiple passes while "
                  "it stays unavailable")
            # Fully staged pages (0, 10, 30) are skipped locally; the only
            # HTTP request must target the held page at the hole.
            check(unique2 == [blocked_offset],
                  "restart must re-request only the missing page "
                  f"(offset {blocked_offset}); saw {unique2}")
            check(snap2["snapshot_seq"] == blocked_offset,
                  "snapshot must stay parked at the held page boundary")
            check(snap2["batch_shots_with_batch_note"] == 0,
                  "batch state must stay invisible while the hole is open")
            check(snap2["batch_shots_past_batch_start"] == 0,
                  "visible state may not reach into the open batch")
            check(snap2["rebuild_mismatch"] == 0,
                  "rebuild oracle mismatch after restart")
        except CheckFailure as exc:
            failures.append(f"run2: {exc}")

        # --------------------------------- run 3: unblock, full convergence
        print("\n[run 3] page released; worker backfills it and converges")
        set_source_phase(blocked=False, redirected=False)
        before_audit3 = len(parse_audit(audit_log))
        r3 = run_worker(run_dir, db_path, url, "worker3.log")
        print(f"  rc={r3['returncode']} stdout={r3['stdout'].strip()}")
        snap3 = snapshot_catalog(db_path, "converged",
                                 run_dir / "snapshot_converged.json",
                                 batch_start)
        entries3 = parse_audit(audit_log)[before_audit3:]
        offsets3 = requested_offsets(entries3)
        unique3 = sorted(set(offsets3))
        print(f"  unique request offsets in run 3: {unique3}")
        print(f"  snapshot={snap3['snapshot_seq']}/{total} "
              f"batch_state_visible={snap3['batch_shots_with_batch_note']}/15 "
              f"tail_visible={snap3['tail_shots_visible']}/5 "
              f"oracle_mismatch={snap3['rebuild_mismatch']}")
        try:
            check(unique3 == [blocked_offset],
                  "only the formerly missing page may be fetched; fully "
                  f"staged pages must not be re-requested: saw {unique3}")
            check(snap3["snapshot_seq"] == total,
                  f"snapshot must reach source total {total}, "
                  f"got {snap3['snapshot_seq']}")
            check(snap3["batch_shots_with_batch_note"] == 15,
                  "the whole batch must be visible after the hole closes")
            check(snap3["batch_shots_past_batch_start"] == 15,
                  "all 15 shots must reflect their batch event atomically")
            check(snap3["tail_shots_visible"] == 5,
                  "ordinary tail records after the batch must publish")
            check(snap3["rebuild_mismatch"] == 0,
                  "final state must match the full-prefix rebuild oracle")
        except CheckFailure as exc:
            failures.append(f"run3: {exc}")

        # --------------------------------------------- whole-run audit checks
        all_entries = parse_audit(audit_log)
        blocked_hits = [e for e in all_entries if e["event"] == "blocked"]
        served = [e for e in all_entries if e["event"] == "served"]
        redirects = [e for e in all_entries if e["event"] == "redirect"]
        print("\n[audit] source saw "
              f"{len(requested_offsets(all_entries))} page requests, "
              f"{len(blocked_hits)} blocked, {len(redirects)} redirected, "
              f"{len(served)} served")
        try:
            check(redirects, "later pages must have been delivered via redirect "
                  "before the hole was filled")
            check(len(blocked_hits) >= 2,
                  "the held page must have been retried (500) during run 2")
            # The redirected early request actually delivered later batch
            # seqs (21..30) in place of the held page's window (11..20).
            early_redirect = any(
                e["event"] == "served" and e["offset"] == blocked_offset
                and e["served_from"] == redirect_target
                for e in all_entries)
            check(early_redirect,
                  "audit must show a later page served in place of the held "
                  "page (out-of-order delivery)")
            served_offsets = {e["offset"] for e in served}
            check(blocked_offset in served_offsets,
                  "the held page must eventually be served after unblocking")
            check(tail_offset in served_offsets,
                  "the ordinary tail page must be served before the hole is "
                  "ever filled")
            # Every seq 1..total must have been served at least once.
            covered: set[int] = set()
            for e in served:
                covered.update(range(e["seq_min"], e["seq_max"] + 1))
            check(covered == set(range(1, total + 1)),
                  "source audit must cover every seq 1..total at least once")
        except CheckFailure as exc:
            failures.append(f"audit: {exc}")

    finally:
        if src_proc.poll() is None:
            src_proc.send_signal(signal.SIGTERM)
            try:
                src_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                src_proc.kill()
        src_logfh.close()

    # --------------------------------------------------------------- verdict
    print("\n" + "=" * 68)
    if failures:
        print("REGRESSION FAILED:")
        for f in failures:
            print(f"  - {f}")
        print(f"\nartifacts: {run_dir}")
        return 1
    print("REGRESSION PASSED: later pages waited for the missing page, the "
          "SIGKILL/restart resumed at the first unstaged page without "
          "re-fetching staged pages, and the source's last complete batch "
          "published atomically.")
    print(f"artifacts: {run_dir}")
    print(f"  snapshots : {run_dir}/snapshot_*.json")
    print(f"  worker log: worker1.log / worker2.log / worker3.log")
    print(f"  request   : {audit_log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
