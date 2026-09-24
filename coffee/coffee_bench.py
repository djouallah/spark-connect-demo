"""Josue Bogran's 17 coffee-shop benchmark queries, run through Spark SQL on Snowpark Connect.

Method from the original README: warm-up `SELECT '1'`, then Query_01..17 one after another,
full results (no row limit). Tables live in TPCH.COFFEE (3-part names, like the original).
The three CTAS queries are made Iceberg with Spark's `USING iceberg`; if Spark SQL can't do
that, the same SELECT is written with the DataFrame API -- pure Spark either way.
"""
import logging, re, time
from pyspark.sql import SparkSession

log = logging.getLogger("quack.coffee_bench")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()

# The .sql file is uploaded next to the dims; read it back whole from the stage.
sql_text = "\n".join(r.value for r in spark.read.text("@TPCH.PUBLIC.COFFEE_RAW/raw/coffee_sql/queries.sql").collect())
parts = re.split(r"^--Query (\d+)\s*$", sql_text, flags=re.M)
queries = {f"Q{n}": body.strip().rstrip(";").replace("sweetcoffeetree.coffeesales500m_v2_small.", "TPCH.COFFEE.")
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
        spark.sql(f"CREATE OR REPLACE TABLE {table} USING iceberg AS {select}").collect()
        how = "spark.sql CTAS USING iceberg"
    except Exception as e:
        log.info("CTAS|%s|spark.sql USING iceberg FAIL|%s", table, f"{type(e).__name__}: {e}".splitlines()[0][:400])
        spark.sql(select).write.format("iceberg").mode("overwrite").saveAsTable(table)
        how = "spark.sql(select).write.format(iceberg)"
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
      .write.format("iceberg").mode("overwrite").saveAsTable("TPCH.COFFEE.TIMINGS_SPARK"))
