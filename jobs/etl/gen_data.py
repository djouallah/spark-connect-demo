"""Generate messy raw files for etl.py, plus the answers the ETL must reproduce.

    python jobs/etl/gen_data.py   ->  jobs/etl/data/{orders,customers,products,fx}/...  and  jobs/etl/data/expected/expected.json
"""
import csv, json, random, re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path(__file__).parent / "data"
DAYS = ["2026-09-01", "2026-09-02", "2026-09-03"]
FX = {"AUD": 0.66, "NZD": 0.60, "USD": 1.0}
DIAL = {"AU": "61", "NZ": "64", "US": "1"}
CITIES = {"AU": ["Sydney", "Melbourne", "Brisbane"], "NZ": ["Auckland", "Wellington"], "US": ["Seattle", "Austin"]}

for sub in ("orders", "customers", "products", "fx", "expected"):
    (OUT / sub).mkdir(parents=True, exist_ok=True)


def messy(s):
    """Random padding and casing -- the ETL has to undo this."""
    s = random.choice([s, s.upper(), s.lower(), s.title()])
    return random.choice(["", " ", "  "]) + s + random.choice(["", " ", "  "])


def e164(phone, country):
    """Reference implementation of what the ETL's phone UDF must produce."""
    digits = re.sub(r"\D", "", phone)
    code = DIAL[country]
    if digits.startswith(code) and phone.strip().startswith("+"):
        return "+" + digits
    return "+" + code + digits.lstrip("0")


def raw_phone(country):
    if country == "US":
        a, b, c = random.randint(200, 999), random.randint(200, 999), random.randint(1000, 9999)
        return random.choice([f"({a}) {b}-{c}", f"{a}.{b}.{c}", f"+1 {a} {b} {c}"])
    mobile = f"4{random.randint(10000000, 99999999)}" if country == "AU" else f"21{random.randint(1000000, 9999999)}"
    code = DIAL[country]
    return random.choice([f"0{mobile[:3]} {mobile[3:6]} {mobile[6:]}", f"+{code}{mobile}",
                          f"(0{mobile[:1]}) {mobile[1:5]}-{mobile[5:]}", f"0{mobile}"])


# ---- products ----
products = [(f"SKU{i:03d}", random.choice(["hardware", "software", "services"]), round(random.uniform(5, 900), 2))
            for i in range(1, 21)]
with open(OUT / "products" / "products.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["sku", "category", "list_price"])
    w.writerows(products)

# ---- fx ----
with open(OUT / "fx" / "fx.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["currency", "rate_to_usd"])
    w.writerows(FX.items())

# ---- customers: 1-3 versions each; the latest version wins (country may change) ----
latest = {}
with open(OUT / "customers" / "customers.jsonl", "w") as f:
    for i in range(1, 301):
        cid = f"C{i:04d}"
        t = datetime(2026, 1, 1) + timedelta(days=random.randint(0, 200))
        versions = []
        for _ in range(random.randint(1, 3)):
            country = random.choice(["AU", "AU", "NZ", "US"])
            t += timedelta(days=random.randint(1, 30), minutes=random.randint(0, 600))
            versions.append({"customer_id": cid, "name": f"Customer {i}",
                             "email": f"customer{i}@example.com",
                             "phone": raw_phone(country),
                             "address": {"city": random.choice(CITIES[country]), "country": country},
                             "updated_at": t.strftime("%Y-%m-%d %H:%M:%S")})
        latest[cid] = versions[-1]
        random.shuffle(versions)                     # file order must not matter
        for v in versions:
            f.write(json.dumps(v) + "\n")

# ---- orders: 1000/day, ~2% exact duplicate lines, ~1% bad rows ----
exp_orders, exp_rejects = 0, 0
agg = defaultdict(lambda: {"orders": 0, "revenue_usd": 0.0, "customers": set()})
n = 0
for day in DAYS:
    rows = []
    for _ in range(1000):
        n += 1
        oid, cid = f"O{n:06d}", f"C{random.randint(1, 300):04d}"
        sku = random.choice(products)[0]
        cur = random.choice(list(FX))
        amt = round(random.uniform(3, 1500), 2)
        ts = datetime.fromisoformat(day) + timedelta(seconds=random.randint(0, 86399))
        ts_txt = random.choice([ts.strftime("%Y-%m-%d %H:%M:%S"), ts.strftime("%d/%m/%Y %H:%M:%S")])
        amt_txt = random.choice([f"${amt:,.2f}", f"{amt}", f" {amt:.2f} "])
        bad = random.random() < 0.01
        if bad:
            if random.random() < 0.5:
                oid = ""
            else:
                ts_txt = "not-a-date"
            exp_rejects += 1
        else:
            exp_orders += 1
            a = agg[(day, latest[cid]["address"]["country"])]
            a["orders"] += 1
            a["revenue_usd"] += amt * FX[cur]
            a["customers"].add(cid)
        row = [messy(oid) if oid else "", messy(cid), messy(sku),
               messy(random.choice(["shipped", "delivered", "pending"])), amt_txt, messy(cur), ts_txt]
        rows.append(row)
        if not bad and random.random() < 0.02:
            rows.append(list(row))                  # exact duplicate line
    random.shuffle(rows)
    with open(OUT / "orders" / f"orders_{day}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "sku", "status", "amount", "currency", "order_ts"])
        w.writerows(rows)

expected = {
    "orders": exp_orders,
    "rejects": exp_rejects,
    "customers": len(latest),
    "daily_country": {f"{d}|{c}": {"orders": v["orders"], "revenue_usd": round(v["revenue_usd"], 2),
                                   "customers": len(v["customers"])}
                      for (d, c), v in sorted(agg.items())},
    "phones": {cid: e164(v["phone"], v["address"]["country"]) for cid, v in latest.items()},
    "countries": {cid: v["address"]["country"] for cid, v in latest.items()},
}
(OUT / "expected" / "expected.json").write_text(json.dumps(expected, indent=1))
print(f"orders={exp_orders} rejects={exp_rejects} customers={len(latest)} "
      f"raw_order_lines={sum(1 for p in (OUT / 'orders').glob('*.csv') for _ in open(p)) - len(DAYS)}")
