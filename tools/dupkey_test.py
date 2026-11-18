import sys
sys.path.insert(0, "/workspace/pylibs/lib/python3.11/site-packages")
import duckdb

con = duckdb.connect()
con.execute("CREATE TABLE t(seq INT PRIMARY KEY, v VARCHAR)")
con.execute("INSERT INTO t VALUES (1, 'a')")
con.execute("CREATE TEMP TABLE s AS SELECT * FROM t LIMIT 0")
con.executemany("INSERT INTO s VALUES (?, ?)", [(1, "b"), (1, "b"), (2, "c")])

# 1) raw merge with duplicated source keys
try:
    con.execute("INSERT INTO t SELECT * FROM s ON CONFLICT(seq) DO UPDATE SET v = EXCLUDED.v")
    print("raw merge ok:", con.execute("SELECT * FROM t ORDER BY seq").fetchall())
except Exception as e:
    print("raw merge error:", type(e).__name__, str(e)[:150])

# 2) dedup then merge
con.execute("CREATE TEMP TABLE d AS SELECT DISTINCT * FROM s")
con.execute("INSERT INTO t SELECT * FROM d ON CONFLICT(seq) DO UPDATE SET v = EXCLUDED.v")
print("dedup merge:", con.execute("SELECT * FROM t ORDER BY seq").fetchall())
