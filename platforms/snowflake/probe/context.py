"""Does setting the session context (USE / setCurrentDatabase) stop Snowpark Connect's
CURRENT_DATABASE() / CURRENT_SCHEMA() chatter? Same 3 queries x 5 under three setups; each setup's
UTC start/end is logged so Query History can be counted per setup afterwards. Pure Spark."""
import datetime as dt, logging, time
from pyspark.sql import SparkSession

log = logging.getLogger("quack.context")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()

QUERIES = {
    "q1": """SELECT l_returnflag, l_linestatus, SUM(l_quantity) q, COUNT(*) n FROM {p}lineitem
             WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus""",
    "q6": """SELECT SUM(l_extendedprice * l_discount) FROM {p}lineitem
             WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
               AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24""",
    "join": """SELECT n.n_name, COUNT(*) FROM {p}customer c JOIN {p}nation n ON c.c_nationkey = n.n_nationkey
               GROUP BY n.n_name""",
}
REPEAT = 5


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def run(setup, prefix, before=None):
    if before:
        before()
    start, t0 = now(), time.time()
    for _ in range(REPEAT):
        for q in QUERIES.values():
            spark.sql(q.format(p=prefix)).collect()
    log.info("SETUP|%s|%s|%s|%.2fs|current=%s", setup, start, now(), time.time() - t0,
             spark.catalog.currentDatabase())
    time.sleep(3)                                         # clean gap between windows


import sys
SETUPS = {
    "1_fully_qualified": ("TPCH.TPCH_SF1.", None),
    "2_use_then_1part": ("", lambda: spark.sql("USE TPCH.TPCH_SF1").collect()),
    "3_setCurrentDatabase_then_1part": ("", lambda: spark.catalog.setCurrentDatabase("TPCH_SF1")),
    "4_use_then_fully_qualified": ("TPCH.TPCH_SF1.", lambda: spark.sql("USE TPCH.TPCH_SF1").collect()),
}
only = sys.argv[sys.argv.index("--only") + 1].split(",") if "--only" in sys.argv else list(SETUPS)
for name in only:
    run(name, *SETUPS[name])
log.info("DONE|context probe")
