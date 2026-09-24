"""Compare the Iceberg tables etl.py wrote against data/expected.json (computed by gen_data.py)."""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sf import connect

exp = json.load(open(Path(__file__).parent / "data" / "expected.json"))
cur = connect().cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

fails = []
def check(name, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got={got} want={want}")
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
    print(f"{'PASS' if ok else 'FAIL'}  {k}: got={g} want={w}")
    if not ok:
        fails.append(k)

print("\nALL PASS" if not fails else f"\n{len(fails)} FAILED: {fails}")
