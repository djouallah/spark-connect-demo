"""Run a job as a Snowflake Code Bundle: upload the job (and its data) to a stage, submit it, print the
result, then the job's logs. Every Snowflake-specific value the jobs need is in JOBS below.

    python platforms/snowflake/run.py simple
    python platforms/snowflake/run.py etl            # then: python platforms/snowflake/check_etl.py
    python platforms/snowflake/run.py tpch_gen --args --sf 100
    python platforms/snowflake/run.py tpch --args --sf 100 --schema TPCH.TPCH_SF100 --raw @TPCH.PUBLIC.TPCH_RAW/gen/sf100_put
"""
import argparse, time
from pathlib import Path
from sf import connect

HERE = Path(__file__).parent
ROOT = HERE.parents[1]

# name: job file, bundle spec, local data dir -> uploaded to @<stage>/raw/<subfolder>/, stage, job arguments, result table
JOBS = {
    "simple":       dict(job="jobs/simple/simple.py", spec="simple", result="SIMPLE_DEMO"),
    "etl":          dict(job="jobs/etl/etl.py", spec="etl", data="jobs/etl/data", stage="CODE_BUNDLE_TEST",
                         args=["--raw", "@TPCH.PUBLIC.CODE_BUNDLE_TEST/raw", "--schema", "TPCH.PUBLIC"],
                         result="ETL_AGG_DAILY_COUNTRY"),
    "coffee_gen":   dict(job="jobs/coffee/coffee_gen.py", spec="coffee", data="jobs/coffee/data", stage="COFFEE_RAW",
                         args=["--raw", "@TPCH.PUBLIC.COFFEE_RAW/raw/coffee_dims", "--schema", "TPCH.COFFEE"]),
    "coffee_bench": dict(job="jobs/coffee/coffee_bench.py", spec="coffee",
                         args=["--sql", "@TPCH.PUBLIC.COFFEE_RAW/raw/coffee_sql/queries.sql", "--schema", "TPCH.COFFEE"],
                         result="TPCH.COFFEE.TIMINGS_SPARK"),
    "dml":          dict(job="jobs/dml/dml.py", spec="dml", args=["--schema", "TPCH.DML", "--tag", "ext"],
                         result="TPCH.DML.RESULTS_EXT"),
    "dml_noext":    dict(job="jobs/dml/dml.py", spec="dml_noext", args=["--schema", "TPCH.DML", "--tag", "noext"],
                         result="TPCH.DML.RESULTS_NOEXT"),
    "tpch_gen":     dict(job="platforms/snowflake/tpch_gen.py", spec="tpch_gen", args=["--stage", "@TPCH.PUBLIC.TPCH_RAW"]),
    "tpch":         dict(job="jobs/tpch/tpch.py", spec="tpch", data="jobs/tpch/data", stage="TPCH_RAW",
                         args=["--sql", "@TPCH.PUBLIC.TPCH_RAW/raw/tpch_sql/tpch.sql"]),
    "probe":        dict(job="platforms/snowflake/probe/probe.py", spec="probe"),
    "probe_context": dict(job="platforms/snowflake/probe/context.py", spec="probe"),
    "probe_mount":  dict(job="platforms/snowflake/probe/mount.py", spec="mount"),
}

p = argparse.ArgumentParser()
p.add_argument("name", choices=JOBS)
p.add_argument("--spec", help="bundle spec to use instead of the job's own")
p.add_argument("--args", nargs=argparse.REMAINDER, default=[], help="extra ARGUMENTS passed to the job")
a = p.parse_args()
cfg = JOBS[a.name]
job = ROOT / cfg["job"]
spec_path = Path(a.spec) if a.spec else HERE / "bundles" / f"{cfg['spec']}.yml"
job_args = cfg.get("args", []) + a.args

con = connect()
cur = con.cursor()


def q(sql):
    cur.execute(sql)
    return cur.fetchall()


db, sch, wh = q("select current_database(), current_schema(), current_warehouse()")[0]
print(f"context: db={db} schema={sch} wh={wh}")

q("create stage if not exists CODE_BUNDLE_TEST")
q(f"put 'file://{job.resolve().as_posix()}' @CODE_BUNDLE_TEST/py/ auto_compress=false overwrite=true")
if cfg.get("data"):
    stage = cfg["stage"]
    q(f"create stage if not exists {stage}")
    for sub in sorted(d for d in (ROOT / cfg["data"]).iterdir() if d.is_dir()):
        q(f"remove @{stage}/raw/{sub.name}/")
        up = q(f"put 'file://{sub.resolve().as_posix()}/*' @{stage}/raw/{sub.name}/ "
               "auto_compress=false overwrite=true")
        print(f"uploaded @{stage}/raw/{sub.name}/: {[r[0] for r in up]}")

spec = spec_path.read_text()
args_sql = f"arguments = ({', '.join(repr(x) for x in job_args)})" if job_args else ""
t0 = time.time()
try:
    cur.execute(f"""
execute code bundle from '@CODE_BUNDLE_TEST/py/'
  entrypoint = '{job.name}'
  {args_sql}
  execution_name = 'quack_{a.name}'
  with specification $${spec}$$""")
    print("execute result:", cur.fetchall())
except Exception as e:
    print("EXECUTE FAILED:", e)
job_id = cur.sfqid
print(f"job query id={job_id} elapsed={time.time()-t0:.1f}s")

for r in q(f"""select status, error_message
               from table(snowflake.information_schema.code_bundle_history(bundle_name => null))
               where query_id = '{job_id}'"""):
    print("history:", r)

if cfg.get("result"):
    try:
        cur.execute(f"select * from {cfg['result']}")
        print(f"\n{cfg['result']}:", [d[0] for d in cur.description])
        for r in cur.fetchall():
            print("  ", r)
    except Exception as e:
        print("result table read failed:", e)

# Job logs: only `logging` output reaches the event table (print/stdout does not).
# The event table can lag a little behind the job finishing.
time.sleep(20)
try:
    rows = q(f"""select record['severity_text']::string, value::string
                 from snowflake.telemetry.events_view
                 where resource_attributes['snow.query.id'] = '{job_id}'
                   and record_type = 'LOG'
                   and (scope['name']::string like 'quack%' or record['severity_text']::string in ('ERROR','FATAL'))
                 order by timestamp""")
    print(f"\njob logs ({len(rows)}):")
    for sev, body in rows:
        print(sev, body[:3000])
except Exception as e:
    print("event log query failed:", e)
