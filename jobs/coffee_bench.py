"""Josue Bogran's 17 coffee-shop benchmark queries, run through Spark SQL.

    --sql <path to queries.sql>  --schema <catalog.schema coffee_gen.py wrote>  [--format iceberg]

Method from the original README: warm-up `SELECT '1'`, then Query_01..17 one after another,
full results (no row limit). Tables are named <schema>.<table>, like the original.
The three CTAS queries use Spark's `USING <format>`; if Spark SQL can't do that, the same
SELECT is written with the DataFrame API -- pure Spark either way.
"""
import logging, re, sys, time
from pyspark.sql import SparkSession

log = logging.getLogger("quack.coffee_bench")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()


def arg(name, default=None):
    """Job argument `--name value`. Every platform-specific value (paths, schema, format) comes in this way."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    if default is None:
        raise SystemExit(f"missing job argument {name}")
    return default

SCHEMA = arg("--schema")
FMT = arg("--format", "iceberg")

sql_text = "\n".join(r.value for r in spark.read.text(arg("--sql")).collect())
parts = re.split(r"^--Query (\d+)\s*$", sql_text, flags=re.M)
queries = {f"Q{n}": body.strip().rstrip(";").replace("sweetcoffeetree.coffeesales500m_v2_small.", f"{SCHEMA}.")
           for n, body in zip(parts[1::2], parts[2::2])}
log.info("PARSED|%d queries: %s", len(queries), ",".join(queries))

CTAS = re.compile(r"^CREATE\s+OR\s+REPLACE\s+TABLE\s+(\S+)\s+AS\s+(.*)$", re.I | re.S)


def run(q):
    """Returns (rows, how)."""
    m = CTAS.match(q)
    if not m:
        return len(spark.sql(q).collect()), "select"
    table, select = m.groups()
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {table} USING {FMT} AS {select}").collect()
        how = f"spark.sql CTAS USING {FMT}"
    except Exception as e:
        log.info("CTAS|%s|spark.sql USING %s FAIL|%s", table, FMT, f"{type(e).__name__}: {e}".splitlines()[0][:400])
        spark.sql(select).write.format(FMT).mode("overwrite").saveAsTable(table)
        how = f"spark.sql(select).write.format({FMT})"
    return spark.table(table).count(), how


results = []
for name, q in queries.items():
    t0 = time.time()
    try:
        rows, how = run(q)
        status, err = "OK", ""
    except Exception as e:
        rows, how, status, err = -1, "", "FAIL", f"{type(e).__name__}: {e}".splitlines()[0][:500]
    secs = round(time.time() - t0, 2)
    results.append((name, status, secs, rows, how, err))
    log.info("QUERY|%s|%s|%.2fs|rows=%d|%s|%s", name, status, secs, rows, how, err)

(spark.createDataFrame(results, "query string, status string, seconds double, row_count long, how string, error string")
      .write.format(FMT).mode("overwrite").saveAsTable(f"{SCHEMA}.TIMINGS_SPARK"))
