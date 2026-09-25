"""Run a job on LakeSail: a local Sail Spark Connect server (no JVM), local files in, tables written to
OneLake through Sail's built-in OneLake Iceberg REST catalog. Every LakeSail-specific value the jobs need
is in JOBS below.

    pip install pysail pyspark-client azure-identity      # plus tpchgen-cli for tpch_gen
    az login                                               # the token comes from the Azure CLI
    python platforms/lakesail/run.py simple
    python platforms/lakesail/run.py etl
    python platforms/lakesail/run.py etl_check

Needs ONELAKE_WAREHOUSE=<workspace id>/<lakehouse id> in the repo-root .env (a schema-enabled lakehouse).
"""
import argparse, logging, os, runpy, sys, time
from pathlib import Path

HERE = Path(__file__).parent
ROOT = HERE.parents[1]
CATALOG = "onelake"


def local(p):
    return (ROOT / p).resolve().as_posix()


# Session conf, the counterpart of spark_conf in the Snowflake bundle specs. Sail follows Spark 4, where ANSI mode is
# on by default: a bad to_timestamp()/cast raises instead of returning NULL. The jobs are Spark 3.5 code (what
# Snowflake runs), so run them with 3.5 semantics.
SPARK_CONF = {"spark.sql.session.timeZone": "UTC", "spark.sql.ansi.enabled": "false"}

# name: job file, job arguments, result table to show
JOBS = {
    "simple":       dict(job="jobs/simple/simple.py", args=["--table", "dbo.simple_demo"],
                         result="dbo.simple_demo"),
    "etl":          dict(job="jobs/etl/etl.py", args=["--raw", local("jobs/etl/data"), "--schema", "spark_demo"],
                         result="spark_demo.etl_agg_daily_country"),
    "etl_check":    dict(job="jobs/etl/check.py",
                         args=["--expected", local("jobs/etl/data/expected/expected.json"), "--schema", "spark_demo"]),
    "coffee_gen":   dict(job="jobs/coffee/coffee_gen.py", args=["--raw", local("jobs/coffee/data/coffee_dims"), "--schema", "coffee"]),
    "coffee_bench": dict(job="jobs/coffee/coffee_bench.py",
                         args=["--sql", local("jobs/coffee/data/coffee_sql/queries.sql"), "--schema", "coffee"],
                         result="coffee.timings_spark"),
    "dml":          dict(job="jobs/dml/dml.py", args=["--schema", "dml", "--tag", "sail"], result="dml.results_sail"),
    "tpch_gen":     dict(job="jobs/tpch/tpch_gen.py", args=["--raw", local("jobs/tpch/data/sf1")]),
    "tpch":         dict(job="jobs/tpch/tpch.py", args=["--raw", local("jobs/tpch/data/sf1"), "--schema", "tpch_sf1",
                                                        "--sql", local("jobs/tpch/data/tpch_sql/tpch.sql")],
                         result="tpch_sf1.timings"),
}

p = argparse.ArgumentParser()
p.add_argument("name", help=f"one of {', '.join(JOBS)}, or a path to any .py job")
p.add_argument("--args", nargs=argparse.REMAINDER, default=[], help="extra arguments passed to the job")
a = p.parse_args()
cfg = JOBS[a.name] if a.name in JOBS else dict(job=str(Path(a.name).resolve()))

for line in open(ROOT / ".env"):
    if "=" in line and not line.lstrip().startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# One Entra token for https://storage.azure.com/ covers both the catalog and the files. Sail has no
# credential vending, so it needs it twice: bearer_token for the catalog, AZURE_STORAGE_TOKEN for storage.
from azure.identity import AzureCliCredential
token = AzureCliCredential().get_token("https://storage.azure.com/.default").token

# Read once at server start: must be set before SparkConnectServer() exists.
os.environ["SAIL_CATALOG__LIST"] = (
    f'[{{type="onelake", name="{CATALOG}", url="{os.environ["ONELAKE_WAREHOUSE"]}", '
    f'api="iceberg", bearer_token="{token}", '
    f'table_cache_type="session", table_cache_ttl_secs=300, '
    f'database_cache_type="session", database_cache_ttl_secs=300}}]'
)
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = CATALOG
os.environ["AZURE_STORAGE_TOKEN"] = token
os.environ.setdefault("RUST_LOG", "error")

from pysail.spark import SparkConnectServer
server = SparkConnectServer()
server.start()
_, port = server.listening_address
os.environ["SPARK_REMOTE"] = f"sc://localhost:{port}"          # the job's getOrCreate() connects here
print(f"sail listening on {port}, catalog {CATALOG} = OneLake {os.environ['ONELAKE_WAREHOUSE']}")

from pyspark.sql import SparkSession
session = SparkSession.builder.getOrCreate()                  # the job's getOrCreate() returns this same session
for k, v in SPARK_CONF.items():
    session.conf.set(k, v)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout)
job = ROOT / cfg["job"]
sys.argv = [str(job)] + cfg.get("args", []) + a.args
t0, status = time.time(), "DONE"
try:
    runpy.run_path(str(job), run_name="__main__")
except SystemExit as e:
    status = f"EXIT {e.code}" if e.code else "DONE"
except Exception as e:
    status = f"FAILED {type(e).__name__}: {' '.join(str(e).split())[:1500]}"
print(f"\njob {a.name}: {status} in {time.time() - t0:.1f}s")

if cfg.get("result") and status == "DONE":
    session.table(cfg["result"]).show(50, truncate=False)
