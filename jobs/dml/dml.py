"""DML / DDL feature matrix -- pure PySpark + Spark SQL (see CLAUDE.md).

    --schema <catalog.schema>  [--tag default] [--only <substring of a check name>]

Every check starts from a fresh 5-row Iceberg table, runs one operation, then compares the table's actual
contents with what Spark semantics say they must be. Outcomes:
  PASS   ran and the data is right
  WRONG  ran but the data is not what Spark would produce
  FAIL   raised an error (the error is the finding)
"""
import logging, sys, time, datetime as dt
from pyspark.sql import SparkSession, functions as F

log = logging.getLogger("quack.dml")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()


def arg(name, default=None):
    """Job argument `--name value`. Every platform-specific value (paths, schema, format) comes in this way."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    if default is None:
        raise SystemExit(f"missing job argument {name}")
    return default


# Which engine runs this script. Every `if ENGINE == ...` below is a difference between engines (findings/<engine>.md).
try:
    from snowflake.snowpark.context import get_active_session
    get_active_session()                         # only Snowflake Code Bundles have a Snowpark session
    ENGINE = "snowflake"
except Exception:
    # Spark SQL version() is the engine's own version: spark.version starts with it on Spark (Fabric adds a suffix), not on Sail
    ENGINE = "spark" if spark.version.startswith(spark.sql("SELECT version()").first()[0].split()[0]) else "sail"

S = arg("--schema")
TAG = arg("--tag", "default")
ONLY = arg("--only", "")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
D = dt.date(2026, 1, 1)
BASE = [(i, chr(96 + i), float(i * 10), D + dt.timedelta(days=i)) for i in range(1, 6)]   # (1,'a',10.0,2026-01-02) ...
COLS = "id int, name string, amount double, dt date"


def fresh(t, rows=BASE):
    """Setup, not a check: a fresh Iceberg table with the 5 base rows."""
    df = spark.createDataFrame(rows, COLS)
    if ENGINE == "sail":
        # NON-STANDARD: Sail can't replace a table, and its Iceberg DELETE/UPDATE/MERGE need merge-on-read,
        # which ALTER TABLE can't set later -- so drop, then create with the properties
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        v2 = df.writeTo(t).using("iceberg")
        for k in ("write.delete.mode", "write.update.mode", "write.merge.mode"):
            v2 = v2.tableProperty(k, "merge-on-read")
        v2.create()
    else:
        df.write.format("iceberg").mode("overwrite").saveAsTable(t)


def state(t, cols="id, amount"):
    return sorted(tuple(r) for r in spark.sql(f"SELECT {cols} FROM {t}").collect())


def view(name, rows, schema):
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)


# ---------------------------------------------------------------- checks
# each: (name, fn(t) -> (actual, expected))
def insert_values(t):
    spark.sql(f"INSERT INTO {t} VALUES (6, 'f', 60.0, DATE'2026-01-07')")
    return state(t), [(i, i * 10.0) for i in range(1, 7)]

def insert_select(t):
    view("src", [(11, "k", 110.0, D)], COLS)
    spark.sql(f"INSERT INTO {t} SELECT * FROM src")
    return state(t), [(i, i * 10.0) for i in range(1, 6)] + [(11, 110.0)]

def insert_overwrite(t):
    view("src", [(7, "g", 70.0, D)], COLS)
    spark.sql(f"INSERT OVERWRITE {t} SELECT * FROM src")
    return state(t), [(7, 70.0)]

def delete_where(t):
    spark.sql(f"DELETE FROM {t} WHERE id IN (1, 2)")
    return state(t), [(3, 30.0), (4, 40.0), (5, 50.0)]

def update_where(t):
    spark.sql(f"UPDATE {t} SET amount = amount * 2 WHERE id = 3")
    return state(t), [(1, 10.0), (2, 20.0), (3, 60.0), (4, 40.0), (5, 50.0)]

def merge_upsert(t):
    view("src", [(3, "c", 33.0, D), (9, "i", 90.0, D)], COLS)
    spark.sql(f"""MERGE INTO {t} t USING src s ON t.id = s.id
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    return state(t), [(1, 10.0), (2, 20.0), (3, 33.0), (4, 40.0), (5, 50.0), (9, 90.0)]

def merge_conditional_delete(t):
    view("src", [(1, "D"), (2, "U"), (8, "I")], "id int, op string")
    spark.sql(f"""MERGE INTO {t} t USING src s ON t.id = s.id
                  WHEN MATCHED AND s.op = 'D' THEN DELETE
                  WHEN MATCHED THEN UPDATE SET t.amount = -1
                  WHEN NOT MATCHED THEN INSERT (id, name, amount, dt) VALUES (s.id, s.op, 80.0, DATE'2026-01-09')""")
    return state(t), [(2, -1.0), (3, 30.0), (4, 40.0), (5, 50.0), (8, 80.0)]

def merge_not_matched_by_source(t):
    view("src", [(1, "a", 11.0, D), (2, "b", 22.0, D)], COLS)
    spark.sql(f"""MERGE INTO {t} t USING src s ON t.id = s.id
                  WHEN MATCHED THEN UPDATE SET t.amount = s.amount
                  WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    return state(t), [(1, 11.0), (2, 22.0)]

def merge_by_source_update(t):
    view("src", [(1, "a", 11.0, D)], COLS)
    spark.sql(f"""MERGE INTO {t} t USING src s ON t.id = s.id
                  WHEN NOT MATCHED BY SOURCE AND t.id > 3 THEN UPDATE SET t.amount = 0""")
    return state(t), [(1, 10.0), (2, 20.0), (3, 30.0), (4, 0.0), (5, 0.0)]

def truncate(t):
    spark.sql(f"TRUNCATE TABLE {t}")
    return state(t), []

def df_append(t):
    w = spark.createDataFrame([(6, "f", 60.0, D)], COLS).write.mode("append")
    w.format("iceberg").saveAsTable(t)
    return state(t), [(i, i * 10.0) for i in range(1, 7)]

def df_insertInto(t):
    spark.createDataFrame([(6, "f", 60.0, D)], COLS).write.insertInto(t)
    return state(t), [(i, i * 10.0) for i in range(1, 7)]

def df_insertInto_overwrite(t):
    spark.createDataFrame([(6, "f", 60.0, D)], COLS).write.insertInto(t, overwrite=True)
    return state(t), [(6, 60.0)]

def writeTo_append(t):
    spark.createDataFrame([(6, "f", 60.0, D)], COLS).writeTo(t).append()
    return state(t), [(i, i * 10.0) for i in range(1, 7)]

def writeTo_overwrite_condition(t):
    # replace rows where id <= 2 with the new rows, keep the rest
    spark.createDataFrame([(1, "a", 1.0, D)], COLS).writeTo(t).overwrite(F.col("id") <= 2)
    return state(t), [(1, 1.0), (3, 30.0), (4, 40.0), (5, 50.0)]

def writeTo_overwritePartitions(t):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.createDataFrame(BASE, COLS).writeTo(t).using("iceberg").partitionedBy(F.col("name")).create()
    # dynamic overwrite: only partition name='b' is replaced
    spark.createDataFrame([(20, "b", 200.0, D)], COLS).writeTo(t).overwritePartitions()
    return state(t), [(1, 10.0), (3, 30.0), (4, 40.0), (5, 50.0), (20, 200.0)]

def insert_overwrite_dynamic_partition(t):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"CREATE TABLE {t} ({COLS}) USING iceberg PARTITIONED BY (name)")
    spark.sql(f"INSERT INTO {t} SELECT * FROM VALUES (1,'a',10.0,DATE'2026-01-02'),(2,'b',20.0,DATE'2026-01-03') AS v(id,name,amount,dt)")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        spark.sql(f"INSERT OVERWRITE {t} SELECT 22, 'b', 220.0, DATE'2026-01-03'")
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    return state(t), [(1, 10.0), (22, 220.0)]

def create_table_partitioned_ddl(t):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"CREATE TABLE {t} ({COLS}) USING iceberg PARTITIONED BY (days(dt))")
    spark.sql(f"INSERT INTO {t} VALUES (1, 'a', 10.0, DATE'2026-01-02')")
    return state(t), [(1, 10.0)]

def ctas_iceberg(t):
    view("src", BASE, COLS)
    spark.sql(f"CREATE OR REPLACE TABLE {t} USING iceberg AS SELECT * FROM src WHERE id > 3")
    return state(t), [(4, 40.0), (5, 50.0)]

def alter_add_column(t):
    spark.sql(f"ALTER TABLE {t} ADD COLUMNS (note string)")
    spark.sql(f"INSERT INTO {t} VALUES (6, 'f', 60.0, DATE'2026-01-07', 'hi')")
    return state(t, "id, note"), [(1, None), (2, None), (3, None), (4, None), (5, None), (6, "hi")]

def alter_rename_column(t):
    spark.sql(f"ALTER TABLE {t} RENAME COLUMN amount TO amt")
    return state(t, "id, amt"), [(i, i * 10.0) for i in range(1, 6)]

def alter_drop_column(t):
    spark.sql(f"ALTER TABLE {t} DROP COLUMN name")
    return sorted(c.lower() for c in spark.table(t).columns), ["amount", "dt", "id"]

def alter_rename_table(t):
    spark.sql(f"DROP TABLE IF EXISTS {t}_RENAMED")
    spark.sql(f"ALTER TABLE {t} RENAME TO {t}_RENAMED")
    return state(f"{t}_RENAMED"), [(i, i * 10.0) for i in range(1, 6)]

def schema_evolution_mergeSchema(t):
    w = (spark.createDataFrame([(6, "f", 60.0, D, "x")], COLS + ", extra string")
         .write.mode("append").option("mergeSchema", "true"))
    w.format("iceberg").saveAsTable(t)
    return state(t, "id, extra"), [(1, None), (2, None), (3, None), (4, None), (5, None), (6, "x")]

def persistent_view(t):
    spark.sql(f"CREATE OR REPLACE VIEW {t}_V AS SELECT id, amount FROM {t} WHERE id <= 2")
    return state(f"{t}_V"), [(1, 10.0), (2, 20.0)]

def drop_table(t):
    spark.sql(f"DROP TABLE {t}")
    return spark.catalog.tableExists(t), False

def _two_snapshots(t):
    """fresh() made snapshot 1 (5 rows); this DELETE makes snapshot 2 (4 rows).
    Returns the epoch seconds between them -- time.time() is timezone-free, unlike the session's timestamps."""
    time.sleep(3)
    epoch = time.time()
    time.sleep(3)
    spark.sql(f"DELETE FROM {t} WHERE id = 1")
    return epoch

FIVE = [(i, i * 10.0) for i in range(1, 6)]

def snapshots_4part(t):
    _two_snapshots(t)
    return spark.table(f"{t}.snapshots").count() >= 2, True

def snapshots_sql_select(t):
    _two_snapshots(t)
    return spark.sql(f"SELECT count(*) AS n FROM {t}.snapshots").first()["n"] >= 2, True

def snapshots_1part_after_use(t):
    _two_snapshots(t)
    spark.sql(f"USE SCHEMA {S}")
    return spark.table(f"{t.split('.')[-1]}.snapshots").count() >= 2, True

def snapshots_read_format_load(t):
    _two_snapshots(t)
    return spark.read.format("iceberg").load(f"{t}.snapshots").count() >= 2, True

def history_metadata(t):
    _two_snapshots(t)
    return spark.table(f"{t}.history").count() >= 2, True

def time_travel_timestamp_as_of_sql(t):
    epoch = _two_snapshots(t)
    iso = dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
    log.info("TT|sql literal %s", iso)
    return state(f"{t} TIMESTAMP AS OF '{iso}'"), FIVE

def time_travel_as_of_timestamp_option(t):
    epoch = _two_snapshots(t)
    log.info("TT|as-of-timestamp ms %d", int(epoch * 1000))
    old = spark.read.option("as-of-timestamp", str(int(epoch * 1000))).format("iceberg").load(t)
    return sorted((r[0], r[1]) for r in old.select("id", "amount").collect()), FIVE

def time_travel_version_as_of(t):
    _two_snapshots(t)
    first = spark.table(f"{t}.snapshots").orderBy("committed_at").first()["snapshot_id"]
    return state(f"{t} VERSION AS OF {first}"), FIVE

def call_rollback_to_snapshot(t):
    _two_snapshots(t)
    first = spark.table(f"{t}.snapshots").orderBy("committed_at").first()["snapshot_id"]
    sch, tbl = t.split(".")[-2:]
    spark.sql(f"CALL system.rollback_to_snapshot('{sch}.{tbl}', {first})")
    return state(t), FIVE


CHECKS = [
    ("insert_values", insert_values),
    ("insert_select", insert_select),
    ("insert_overwrite", insert_overwrite),
    ("delete_where", delete_where),
    ("update_where", update_where),
    ("merge_upsert (UPDATE SET * / INSERT *)", merge_upsert),
    ("merge_conditional_delete", merge_conditional_delete),
    ("merge_not_matched_by_source_delete", merge_not_matched_by_source),
    ("merge_not_matched_by_source_update", merge_by_source_update),
    ("truncate", truncate),
    ("df.write append saveAsTable", df_append),
    ("df.write.insertInto", df_insertInto),
    ("df.write.insertInto overwrite", df_insertInto_overwrite),
    ("writeTo.append", writeTo_append),
    ("writeTo.overwrite(condition)", writeTo_overwrite_condition),
    ("writeTo.overwritePartitions", writeTo_overwritePartitions),
    ("INSERT OVERWRITE dynamic partition", insert_overwrite_dynamic_partition),
    ("CREATE TABLE USING iceberg PARTITIONED BY days()", create_table_partitioned_ddl),
    ("CTAS USING iceberg", ctas_iceberg),
    ("ALTER TABLE ADD COLUMNS", alter_add_column),
    ("ALTER TABLE RENAME COLUMN", alter_rename_column),
    ("ALTER TABLE DROP COLUMN", alter_drop_column),
    ("ALTER TABLE RENAME TO", alter_rename_table),
    ("append with mergeSchema", schema_evolution_mergeSchema),
    ("CREATE VIEW (persistent)", persistent_view),
    ("DROP TABLE", drop_table),
    ("metadata spark.table(db.sch.t.snapshots)", snapshots_4part),
    ("metadata SELECT FROM db.sch.t.snapshots", snapshots_sql_select),
    ("metadata 1-part t.snapshots after USE", snapshots_1part_after_use),
    ("metadata read.format(iceberg).load(t.snapshots)", snapshots_read_format_load),
    ("metadata .history", history_metadata),
    ("time travel TIMESTAMP AS OF (SQL)", time_travel_timestamp_as_of_sql),
    ("time travel as-of-timestamp read option", time_travel_as_of_timestamp_option),
    ("time travel VERSION AS OF (needs .snapshots)", time_travel_version_as_of),
    ("CALL system.rollback_to_snapshot (needs .snapshots)", call_rollback_to_snapshot),
]

results = []
for i, (name, fn) in enumerate(CHECKS):
    if ONLY and ONLY not in name:
        continue
    t = f"{S}.T_{TAG.upper()}_{i:02d}"
    t0 = time.time()
    try:
        if name.startswith("CREATE TABLE") or name.startswith("CTAS"):
            pass                                   # these create their own table
        else:
            fresh(t)
        actual, expected = fn(t)
        status = "PASS" if actual == expected else "WRONG"
        detail = "" if status == "PASS" else f"got={actual} want={expected}"
    except Exception as e:
        msg = " ".join(l.strip() for l in str(e).splitlines() if l.strip() and not l.startswith("==="))
        status, detail = "FAIL", f"{type(e).__name__}: {msg}"[:400]
    secs = round(time.time() - t0, 1)
    results.append((name, status, secs, detail[:500]))
    log.info("DML|%s|%s|%.1fs|%s", name, status, secs, detail[:300])

res = spark.createDataFrame(results, "feature string, status string, seconds double, detail string")
if ENGINE == "sail":        # NON-STANDARD: Sail can't replace a table on the OneLake Iceberg catalog, so drop it and create it
    spark.sql(f"DROP TABLE IF EXISTS {S}.RESULTS_{TAG.upper()}")
    res.write.format("iceberg").saveAsTable(f"{S}.RESULTS_{TAG.upper()}")
else:
    res.write.format("iceberg").mode("overwrite").saveAsTable(f"{S}.RESULTS_{TAG.upper()}")
log.info("DONE|%s|" % TAG + "%d checks, %d pass, %d wrong, %d fail", len(results),
         sum(r[1] == "PASS" for r in results), sum(r[1] == "WRONG" for r in results), sum(r[1] == "FAIL" for r in results))
