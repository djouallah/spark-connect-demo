"""TPC-H load + 22 queries, pure PySpark / Spark SQL (see CLAUDE.md).

    --raw <dir with one parquet folder per table>  --sql <path to tpch.sql>  --schema <catalog.schema>
    [--sf 1] [--format iceberg] [--force]

0. CREATE SCHEMA + USE it (on Snowflake, without USE Snowpark Connect sends ~17 CURRENT_DATABASE/SCHEMA
   round trips per query). Tables already loaded with the right row count are skipped (idempotent;
   --force reloads everything).
1. Load each missing table from <raw>/<table>/ (parquet) into <schema>.<table>.
2. Run the 22 queries (lakehouse_benchmark's sql/tpch.sql) with spark.sql(...).collect().
3. Timings -> <schema>.TIMINGS / LOAD_TIMINGS.
"""
import logging, re, sys, time
from pyspark.sql import SparkSession

log = logging.getLogger("quack.tpch")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()


def arg(name, default=None):
    """Job argument `--name value`. Every platform-specific value (paths, schema, format) comes in this way."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    if default is None:
        raise SystemExit(f"missing job argument {name}")
    return default


RAW = arg("--raw").rstrip("/")
S = arg("--schema")
SF = int(arg("--sf", "1"))
FMT = arg("--format", "iceberg")
FORCE = "--force" in sys.argv
TABLES = ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")
# TPC-H cardinalities; lineitem varies slightly by SF (6,001,215 at SF1, 600,037,902 at SF100)
EXPECTED = {"orders": 1_500_000 * SF, "partsupp": 800_000 * SF, "part": 200_000 * SF, "customer": 150_000 * SF,
            "supplier": 10_000 * SF, "nation": 25, "region": 5, "lineitem": 6_000_000 * SF}

# ---- 0. schema, context, what is already loaded ----
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.sql(f"USE {S}")


def loaded(t):
    """Row count if the table exists with a plausible TPC-H count, else None."""
    if FORCE or not spark.catalog.tableExists(f"{S}.{t}"):
        return None
    n = spark.table(f"{S}.{t}").count()
    ok = abs(n - EXPECTED[t]) <= EXPECTED[t] * 0.001 if t == "lineitem" else n == EXPECTED[t]
    return n if ok else None


have = {t: loaded(t) for t in TABLES}
todo = [t for t in TABLES if have[t] is None]
for t in TABLES:
    log.info("CHECK|%s|%s", t, f"already loaded, {have[t]:,} rows -> skip" if have[t] is not None else "missing -> load")

# ---- 1. raw parquet -> tables (only what is missing) ----
load_rows = []
for t in todo:
    t0 = time.time()
    spark.read.parquet(f"{RAW}/{t}/").write.format(FMT).mode("overwrite").saveAsTable(f"{S}.{t}")
    n = spark.table(f"{S}.{t}").count()
    secs = round(time.time() - t0, 2)
    load_rows.append((t, n, secs))
    log.info("LOAD|%s|rows=%d|load %.2fs", t, n, secs)

if load_rows:                                  # keep the previous LOAD_TIMINGS when nothing was reloaded
    (spark.createDataFrame(load_rows, "tbl string, row_count long, load_s double")
          .write.format(FMT).mode("overwrite").saveAsTable(f"{S}.LOAD_TIMINGS"))

# ---- 2. the 22 queries through Spark SQL; only the timings are saved ----
raw = "\n".join(r.value for r in spark.read.text(arg("--sql")).collect())
sql = raw.replace("{schema}", S).replace("{SF}", str(SF))
sql = re.sub(rf"`{re.escape(S)}\.(\w+)`", rf"{S}.\1", sql)          # backticked -> dotted, Spark style
queries = [q.strip() for q in sql.split(";") if q.strip()]
assert len(queries) == 22, f"expected 22 statements, found {len(queries)}"

results = []
for i, q in enumerate(queries, 1):
    name = f"Q{i:02d}"
    t0 = time.time()
    try:
        rows = len(spark.sql(q).collect())            # full result, no row limit; only the timings are saved
        status, error = "OK", ""
    except Exception as e:
        msg = " ".join(l.strip() for l in str(e).splitlines() if l.strip() and not l.startswith("==="))
        rows, status, error = -1, "FAIL", f"{type(e).__name__}: {msg}"[:500]
    secs = round(time.time() - t0, 2)
    results.append((name, status, secs, rows, error))
    log.info("QUERY|%s|%s|%.2fs|rows=%d|%s", name, status, secs, rows, error)

(spark.createDataFrame(results, "query string, status string, seconds double, row_count long, error string")
      .write.format(FMT).mode("overwrite").saveAsTable(f"{S}.TIMINGS"))
log.info("DONE|sf=%d|loaded %d tables, skipped %d|%d/22 ok|total query s=%.1f", SF, len(todo), len(TABLES) - len(todo),
         sum(r[1] == "OK" for r in results), sum(r[2] for r in results))
