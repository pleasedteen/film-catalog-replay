import sys, tempfile, os, time, json
sys.path[:0] = ["src", "/workspace/pylibs/lib/python3.11/site-packages"]
import duckdb
from shot_catalog.log_format import normalize
from shot_catalog.catalog import (
    STAGING_COLUMNS, STATE_COLUMNS, _ATTRS, _LATEST_SEQS, _JOIN_CLAUSES,
    _JOIN_SELECTS, _DELTA_GROUP_SQL,
)

base = [normalize(json.loads(l)).as_staging_tuple() for l in open("fixtures/large_history.jsonl") if l.strip()]
set_clauses = ", ".join(
    f"{a} = CASE WHEN d.{a}_changed THEN d.d_{a} ELSE t.{a} END" for a in _ATTRS)

with tempfile.TemporaryDirectory() as d:
    conn = duckdb.connect(os.path.join(d, "t.duckdb"))
    conn.execute(f"CREATE TABLE event_staging ({STAGING_COLUMNS})")
    conn.execute(f"CREATE TABLE shot_state ({STATE_COLUMNS})")
    conn.execute("CREATE INDEX idx_shot_film ON shot_state(film_id)")
    conn.execute("CREATE TABLE sync_state (id INTEGER PRIMARY KEY, last_snapshot_seq BIGINT)")
    conn.execute("INSERT INTO sync_state VALUES (1,0)")
    with conn.appender("event_staging") as ap:
        for r in base:
            ap.append_row(*r)

    def apply_group(start, end, batch, do_cp):
        conn.execute("DROP TABLE IF EXISTS _delta")
        conn.execute(_DELTA_GROUP_SQL.format(
            latest_seqs=_LATEST_SEQS, join_selects=_JOIN_SELECTS, joins=_JOIN_CLAUSES),
            {"start_seq": start, "end_seq": end})
        conn.execute("BEGIN TRANSACTION")
        conn.execute(f"""UPDATE shot_state t SET version=d.dv, last_event_seq=d.dlast,
            last_ts=d.dts, {set_clauses},
            withdrawn=CASE WHEN d.dwd IS NULL THEN t.withdrawn ELSE d.dwd END
            FROM _delta d WHERE t.film_id=d.film_id AND t.shot_id=d.shot_id""")
        conn.execute("""INSERT INTO shot_state (film_id,shot_id,version,last_event_seq,last_ts,
            title,description,duration_ms,scene,camera,repair_note,withdrawn)
            SELECT d.film_id,d.shot_id,d.dv,d.dlast,d.dts,d.d_title,d.d_description,d.d_duration_ms,
            d.d_scene,d.d_camera,d.d_repair_note,COALESCE(d.dwd,false) FROM _delta d
            WHERE NOT EXISTS (SELECT 1 FROM shot_state t WHERE t.film_id=d.film_id AND t.shot_id=d.shot_id)""")
        conn.execute("UPDATE sync_state SET last_snapshot_seq=? WHERE id=1", [end])
        conn.execute("COMMIT")
        if do_cp:
            conn.execute("CHECKPOINT")

    # ordinary groups of 5000 over first 100k
    t0 = time.time()
    for s in range(1, 100001, 5000):
        apply_group(s, min(s + 4999, 100000), None, False)
    print(f"20 ordinary groups (no CP): {time.time()-t0:.1f}s", flush=True)

    t0 = time.time()
    apply_group(100001, 200000, "B900", False)
    print(f"100k batch apply (no CP): {time.time()-t0:.1f}s", flush=True)

    t0 = time.time()
    apply_group(200001, 200004, None, False)
    print(f"tail group: {time.time()-t0:.1f}s", flush=True)

    t0 = time.time(); conn.execute("CHECKPOINT"); print(f"single final checkpoint: {time.time()-t0:.1f}s", flush=True)
    print("visible:", conn.execute("SELECT count(*) FROM shot_state WHERE NOT withdrawn").fetchone()[0], flush=True)
    conn.close()
