import sys, tempfile, os, time, json
sys.path[:0] = ["src", "/workspace/pylibs/lib/python3.11/site-packages"]
import duckdb
from shot_catalog.fixturegen import generate_large_history
from shot_catalog.log_format import normalize
from shot_catalog.catalog import (
    STAGING_COLUMNS, _LATEST_SEQS, _JOIN_CLAUSES, _JOIN_SELECTS, _DELTA_GROUP_SQL,
)

p = "/tmp/h5k.jsonl"
generate_large_history(p, n_shots=5_000)
rows = [normalize(json.loads(l)).as_staging_tuple() for l in open(p) if l.strip()]
COLS = [c.strip().split()[0] for c in STAGING_COLUMNS.strip().rstrip(",").split(",")]


def stage_in_chunks(conn, rows, page):
    n = len(rows)
    for i in range(0, n, page):
        conn.execute("CREATE TEMP TABLE _in AS SELECT * FROM event_staging LIMIT 0")
        with conn.appender("_in") as ap:
            for r in rows[i:i + page]:
                ap.append_row(*r)
        conn.execute("CREATE TEMP TABLE _dd AS SELECT DISTINCT * FROM _in")
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLS if c != "seq")
        conn.execute(f"INSERT INTO event_staging SELECT * FROM _dd "
                     f"ON CONFLICT (seq) DO UPDATE SET {sets}")
        conn.execute("DROP TABLE _in"); conn.execute("DROP TABLE _dd")


def delta_time(conn, start, end):
    conn.execute("DROP TABLE IF EXISTS _delta")
    t = time.time()
    conn.execute(_DELTA_GROUP_SQL.format(
        latest_seqs=_LATEST_SEQS, join_selects=_JOIN_SELECTS, joins=_JOIN_CLAUSES),
        {"start_seq": start, "end_seq": end})
    return time.time() - t


with tempfile.TemporaryDirectory() as d:
    conn = duckdb.connect(os.path.join(d, "frag.duckdb"))
    conn.execute(f"CREATE TABLE event_staging ({STAGING_COLUMNS})")
    t = time.time(); stage_in_chunks(conn, rows, 100)
    print(f"fragmented stage (page=100, {len(rows)} rows): {time.time()-t:.2f}s", flush=True)
    print(f"  delta 1..5000 fragmented: {delta_time(conn,1,5000):.2f}s", flush=True)
    print(f"  db size: {os.path.getsize(os.path.join(d,'frag.duckdb'))//1024} KB", flush=True)

    # compact: rebuild table ordered by seq
    t = time.time()
    conn.execute("CREATE TABLE _es_compact AS SELECT * FROM event_staging ORDER BY seq")
    conn.execute("DROP TABLE event_staging")
    conn.execute("ALTER TABLE _es_compact RENAME TO event_staging")
    print(f"  compaction: {time.time()-t:.2f}s", flush=True)
    print(f"  delta 1..5000 compacted: {delta_time(conn,1,5000):.2f}s", flush=True)

    # big range scan speed on fragmented vs compact
    t = time.time()
    conn.execute("SELECT count(*) FROM event_staging WHERE seq BETWEEN 5001 AND 10000").fetchall()
    print(f"  range scan compacted: {time.time()-t:.3f}s", flush=True)
    conn.close()

    # Baseline: bulk-appender load in ONE shot (no fragmentation)
    conn = duckdb.connect(os.path.join(d, "bulk.duckdb"))
    conn.execute(f"CREATE TABLE event_staging ({STAGING_COLUMNS})")
    t = time.time()
    with conn.appender("event_staging") as ap:
        for r in rows:
            ap.append_row(*r)
    print(f"bulk one-shot stage: {time.time()-t:.2f}s", flush=True)
    print(f"  delta 1..5000 bulk: {delta_time(conn,1,5000):.2f}s", flush=True)
    conn.close()
