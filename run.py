"""Upload a PySpark job (and optionally raw data) to a stage and run it as a Snowflake Code Bundle.
The bundle spec is code_bundle.yml next to the job.

    python run.py simple/job.py --result CODE_BUNDLE_DEMO
    python run.py etl/etl.py --result ETL_AGG_DAILY_COUNTRY --data-dir etl/data
"""
import argparse, time
from pathlib import Path
from sf import connect

p = argparse.ArgumentParser()
p.add_argument("job")
p.add_argument("--spec", help="default: code_bundle.yml next to the job")
p.add_argument("--result", help="table to print after the job")
p.add_argument("--data-dir", help="each subfolder is uploaded to @<stage>/raw/<subfolder>/")
p.add_argument("--stage", default="CODE_BUNDLE_TEST", help="stage for --data-dir uploads (job code always goes to CODE_BUNDLE_TEST)")
p.add_argument("--args", nargs=argparse.REMAINDER, default=[], help="ARGUMENTS passed to the job")
a = p.parse_args()
job = Path(a.job)
spec_path = Path(a.spec) if a.spec else job.parent / "code_bundle.yml"

con = connect()
cur = con.cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

db, sch, wh = q("select current_database(), current_schema(), current_warehouse()")[0]
print(f"context: db={db} schema={sch} wh={wh}")

q("create stage if not exists CODE_BUNDLE_TEST")
q(f"put 'file://{job.resolve().as_posix()}' @CODE_BUNDLE_TEST/py/ auto_compress=false overwrite=true")
if a.data_dir:
    for sub in sorted(d for d in Path(a.data_dir).iterdir() if d.is_dir()):
        q(f"create stage if not exists {a.stage}")
        q(f"remove @{a.stage}/raw/{sub.name}/")
        up = q(f"put 'file://{sub.resolve().as_posix()}/*' @{a.stage}/raw/{sub.name}/ "
               "auto_compress=false overwrite=true")
        print(f"uploaded @{a.stage}/raw/{sub.name}/: {[r[0] for r in up]}")

spec = spec_path.read_text()
args_sql = f"arguments = ({', '.join(repr(x) for x in a.args)})" if a.args else ""
t0 = time.time()
try:
    cur.execute(f"""
execute code bundle from '@CODE_BUNDLE_TEST/py/'
  entrypoint = '{job.name}'
  {args_sql}
  execution_name = 'quack_{job.stem}'
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

if a.result:
    try:
        cur.execute(f"select * from {a.result}")
        print(f"\n{a.result}:", [d[0] for d in cur.description])
        for r in cur.fetchall():
            print("  ", r)
    except Exception as e:
        print("result table read failed:", e)

try:
    print("\niceberg tables:", [f"{r[3]}.{r[1]}" for r in q("show iceberg tables in database TPCH")])
except Exception as e:
    print("show iceberg tables failed:", e)

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
