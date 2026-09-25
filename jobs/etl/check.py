"""Check the tables etl.py wrote against expected.json (computed independently by gen_data.py, in plain Python).

    --expected <path to expected.json>  --schema <catalog.schema etl.py wrote to>

Fails the job (SystemExit) if any check fails.
"""
import json, logging, sys
from pyspark.sql import SparkSession

log = logging.getLogger("quack.etl_check")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()


def arg(name, default=None):
    """Job argument `--name value`. Every platform-specific value (paths, schema, format) comes in this way."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    if default is None:
        raise SystemExit(f"missing job argument {name}")
    return default


exp = json.loads("\n".join(r.value for r in spark.read.text(arg("--expected")).collect()))
spark.sql(f"USE {arg('--schema')}")


def q(sql):
    return [tuple(r) for r in spark.sql(sql).collect()]


fails = []
def check(name, got, want):
    ok = got == want
    log.info("CHECK|%s|%s|got=%s want=%s", "PASS" if ok else "FAIL", name, got, want)
    if not ok:
        fails.append(name)

check("fct_orders rows", q("select count(*) from ETL_FCT_ORDERS")[0][0], exp["orders"])
check("fct_orders distinct order_id", q("select count(distinct order_id) from ETL_FCT_ORDERS")[0][0], exp["orders"])
check("dim_customer rows", q("select count(*) from ETL_DIM_CUSTOMER")[0][0], exp["customers"])

dim = {cid: (country, phone) for cid, country, phone in
       q("select customer_id, country, phone_e164 from ETL_DIM_CUSTOMER")}
check("latest-version country per customer", sum(dim[c][0] == v for c, v in exp["countries"].items()), exp["customers"])
check("phone_e164 per customer (UDF)", sum(dim[c][1] == v for c, v in exp["phones"].items()), exp["customers"])

got = {f"{d}|{c}": (n, float(r), k) for d, c, n, r, k in
       q("select order_date, country, orders, revenue_usd, customers from ETL_AGG_DAILY_COUNTRY")}
want = {k: (v["orders"], v["revenue_usd"], v["customers"]) for k, v in exp["daily_country"].items()}
check("agg groups", sorted(got), sorted(want))
for k in sorted(want):
    g, w = got.get(k), want[k]
    ok = g is not None and g[0] == w[0] and g[2] == w[2] and abs(g[1] - w[1]) <= 0.01
    log.info("CHECK|%s|%s|got=%s want=%s", "PASS" if ok else "FAIL", k, g, w)
    if not ok:
        fails.append(k)

log.info("DONE|%s", "ALL PASS" if not fails else f"{len(fails)} FAILED: {fails}")
if fails:
    raise SystemExit(f"{len(fails)} checks failed: {fails}")
