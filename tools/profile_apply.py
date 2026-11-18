import sys, tempfile, os, time, json
sys.path[:0] = ["src", "/workspace/pylibs/lib/python3.11/site-packages"]
import duckdb
from shot_catalog.fixturegen import generate_large_history
from shot_catalog.log_format import normalize
from shot_catalog.catalog import (
    STAGING_COLUMNS, STATE_COLUMNS, _ATTRS, _LATEST_SEQS, _JOIN_CLAUSES,
    _JOIN_SELECTS, _DELTA_GROUP_SQL,
)

p = "/tmp/h5k.jsonl"
generate_large_history(p, n_shots=5_000)
rows = [normalize(json.loads(l)).as_staging_tuple() for l in open(p) if l.strip()]

with tempfile.TemporaryDirectory() as d:
    conn = duckdb.connect(os.path.join(d, "t.duckdb"))
    conn.execute(f"CREATE TABLE event_staging ({STAGING_COLUMNS})")
    conn.execute(f"CREATE TABLE shot_state ({STATE_COLUMNS})")
    conn.execute("CREATE TABLE sync_state (id INTEGER PRIMARY KEY, last_snapshot_seq BIGINT)")
    conn.execute("INSERT INTO sync_state VALUES (1, 0)")
    conn.execute("CREATE INDEX idx_shot_film ON shot_state(film_id)")
    with conn.appender("event_staging") as ap:
        for r in rows:
            ap.append_row(*r)
    print("staged", len(rows), flush=True)

    set_clauses = ", ".join(
        f"{a} = CASE WHEN d.{a}_changed THEN d.d_{a} ELSE t.{a} END" for a in _ATTRS)

    def delta(start, end):
        conn.execute("DROP TABLE IF EXISTS _delta")
        conn.execute(_DELTA_GROUP_SQL.format(
            latest_seqs=_LATEST_SEQS, join_selects=_JOIN_SELECTS, joins=_JOIN_CLAUSES),
            {"start_seq": start, "end_seq": end})

    N = len(rows)
    # Simulate ordinary flush: first 5000 rows (the v1 creates), range 1..5000
    t = time.time(); delta(1, 5000); print(f"delta 1..5000: {time.time()-t:.2f}s", flush=True)
    t = time.time()
    conn.execute("BEGIN TRANSACTION")
    conn.execute(f"""
        UPDATE shot_state AS t
        SET version=d.dv, last_event_seq=d.dlast, last_ts=d.dts, {set_clauses},
            withdrawn = CASE WHEN d.dwd IS NULL THEN t.withdrawn ELSE d.dwd END
        FROM _delta d WHERE t.film_id=d.film_id AND t.shot_id=d.shot_id""")
    print(f"update (empty target): {time.time()-t:.2f}s", flush=True)
    t = time.time()
    conn.execute("""
        INSERT INTO shot_state (film_id,shot_id,version,last_event_seq,last_ts,
          title,description,duration_ms,scene,camera,repair_note,withdrawn)
        SELECT d.film_id,d.shot_id,d.dv,d.dlast,d.dts,
          d.d_title,d.d_description,d.d_duration_ms,d.d_scene,d.d_camera,d.d_repair_note,
          COALESCE(d.dwd,false)
        FROM _delta d WHERE NOT EXISTS (SELECT 1 FROM shot_state t
          WHERE t.film_id=d.film_id AND t.shot_id=d.shot_id)""")
    print(f"insert 5000: {time.time()-t:.2f}s", flush=True)
    conn.execute("UPDATE sync_state SET last_snapshot_seq=5000 WHERE id=1")
    conn.execute("COMMIT")
    t = time.time(); conn.execute("CHECKPOINT"); print(f"checkpoint: {time.time()-t:.2f}s", flush=True)

    # Now the batch group 5001..10000 (5000 existing shots)
    t = time.time(); delta(5001, 10000); print(f"delta 5001..10000: {time.time()-t:.2f}s", flush=True)
    t = time.time()
    conn.execute("BEGIN TRANSACTION")
    conn.execute(f"""
        UPDATE shot_state AS t
        SET version=d.dv, last_event_seq=d.dlast, last_ts=d.dts, {set_clauses},
            withdrawn = CASE WHEN d.dwd IS NULL THEN t.withdrawn ELSE d.dwd END
        FROM _delta d WHERE t.film_id=d.film_id AND t.shot_id=d.shot_id""")
    print(f"update 5000 existing: {time.time()-t:.2f}s", flush=True)
    conn.execute("COMMIT")
    t = time.time(); conn.execute("CHECKPOINT"); print(f"checkpoint: {time.time()-t:.2f}s", flush=True)
