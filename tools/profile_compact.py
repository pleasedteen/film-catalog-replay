import sys, tempfile, os, time, json, random
sys.path[:0] = ["src", "/workspace/pylibs/lib/python3.11/site-packages"]
import duckdb
from shot_catalog.fixturegen import generate_large_history
from shot_catalog.log_format import normalize
from shot_catalog.catalog import (
    STAGING_COLUMNS, _LATEST_SEQS, _JOIN_CLAUSES, _JOIN_SELECTS, _DELTA_GROUP_SQL,
)

p = "/tmp/h20k.jsonl"
generate_large_history(p, n_shots=10_000)  # 20004 events
base = [normalize(json.loads(l)).as_staging_tuple() for l in open(p) if l.strip()]

with tempfile.TemporaryDirectory() as d:
    conn = duckdb.connect(os.path.join(d, "t.duckdb"))
    conn.execute(f"CREATE TABLE event_staging ({STAGING_COLUMNS})")

    # Simulate shuffled, 35%-duplicated, paged appends
    rng = random.Random(7)
    pages = [base[i:i + 1000] for i in range(0, len(base), 1000)]
    t = time.time()
    for pg in pages:
        buf = list(pg)
        for r in pg:
            if rng.random() < 0.35:
                buf.append(r)
        rng.shuffle(buf)
        seen = {}
        for r in buf:
            seen[r[0]] = r
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _in AS SELECT * FROM event_staging LIMIT 0")
        conn.execute("TRUNCATE TABLE _in")
        with conn.appender("_in") as ap:
            for r in seen.values():
                ap.append_row(*r)
        conn.execute("INSERT INTO event_staging SELECT s.* FROM _in s WHERE NOT EXISTS "
                     "(SELECT 1 FROM event_staging e WHERE e.seq=s.seq)")
    print(f"chaos staging: {time.time()-t:.2f}s rows={conn.execute('SELECT count(*) FROM event_staging').fetchone()[0]}", flush=True)

    def delta(start, end):
        conn.execute("DROP TABLE IF EXISTS _delta")
        conn.execute(_DELTA_GROUP_SQL.format(
            latest_seqs=_LATEST_SEQS, join_selects=_JOIN_SELECTS, joins=_JOIN_CLAUSES),
            {"start_seq": start, "end_seq": end})

    # big batch range = 10001..20000 on fragmented table
    t = time.time(); delta(10001, 20000); print(f"delta 10k batch FRAGMENTED: {time.time()-t:.2f}s", flush=True)

    # compact
    t = time.time()
    conn.execute("BEGIN TRANSACTION")
    conn.execute("CREATE TABLE _es AS SELECT * FROM event_staging ORDER BY seq")
    conn.execute("DROP TABLE event_staging")
    conn.execute("ALTER TABLE _es RENAME TO event_staging")
    conn.execute("COMMIT")
    print(f"compaction: {time.time()-t:.2f}s", flush=True)

    t = time.time(); delta(10001, 20000); print(f"delta 10k batch COMPACTED: {time.time()-t:.2f}s", flush=True)
    conn.close()
