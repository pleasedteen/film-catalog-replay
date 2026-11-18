import sys, tempfile, os, time, json
sys.path[:0] = ["src", "/workspace/pylibs/lib/python3.11/site-packages"]
import duckdb
from shot_catalog.fixturegen import generate_large_history
from shot_catalog.log_format import normalize
from shot_catalog.catalog import STAGING_COLUMNS, _LATEST_SEQS, _JOIN_CLAUSES, _JOIN_SELECTS, _DELTA_GROUP_SQL

p = "/tmp/h5k.jsonl"
generate_large_history(p, n_shots=5_000)
rows = [normalize(json.loads(l)).as_staging_tuple() for l in open(p) if l.strip()]

with tempfile.TemporaryDirectory() as d:
    conn = duckdb.connect(os.path.join(d, "a.duckdb"))
    # append-only staging: no PK, just an index on seq
    conn.execute(f"CREATE TABLE event_staging ({STAGING_COLUMNS.replace('PRIMARY KEY','')})")
    conn.execute("CREATE INDEX es_seq ON event_staging(seq)")
    t = time.time()
    # first delivery
    with conn.appender("event_staging") as ap:
        for r in rows:
            ap.append_row(*r)
    t1 = time.time()
    # simulate 35% redelivery (duplicates appended) interleaved
    import random
    rng = random.Random(1)
    redel = [r for r in rows if rng.random() < 0.35]
    with conn.appender("event_staging") as ap:
        for r in redel:
            ap.append_row(*r)
    t2 = time.time()
    print(f"append first {len(rows)}: {t1-t:.2f}s; redeliver {len(redel)}: {t2-t1:.2f}s", flush=True)
    print("total physical rows:", conn.execute("SELECT count(*) FROM event_staging").fetchone()[0], flush=True)

    # delta must dedupe by seq taking the latest physical copy. Group by on
    # duplicated table: verify correctness with an explicit dedupe CTE.
    conn.execute("DROP TABLE IF EXISTS _delta")
    t = time.time()
    conn.execute("""
        CREATE TEMP TABLE _uniq AS
        SELECT * FROM event_staging QUALIFY row_number() OVER (PARTITION BY seq ORDER BY seq) = 1
    """)
    # simpler: use the latest row per seq
    conn.execute("DROP TABLE _uniq")
    conn.execute("""
        CREATE TEMP TABLE _uniq AS
        SELECT DISTINCT ON (seq) * FROM event_staging
    """)
    print(f"dedupe to uniq: {time.time()-t:.2f}s uniq rows={conn.execute('SELECT count(*) FROM _uniq').fetchone()[0]}", flush=True)
    conn.close()
