"""Run a job on Fabric Spark as a Spark Job Definition. Fabric has no Spark Connect endpoint, so this is a
submit: upload the job (and its data) to OneLake Files, point the SJD at it, run it, wait, then print the
job's output. Every Fabric-specific value the jobs need is in JOBS below.

    pip install azure-identity requests
    az login                                               # the tokens come from the Azure CLI
    python platforms/fabric/run.py simple
    python platforms/fabric/run.py etl
    python platforms/fabric/run.py etl_check

Needs FABRIC_SPARK_LAKEHOUSE=<workspace id>/<lakehouse id> in the repo-root .env: a schema-enabled lakehouse, its own
(not LakeSail's), so the same schema and table names never clash.
"""
import argparse, base64, json, os, time
from pathlib import Path
import requests
from azure.identity import AzureCliCredential

HERE = Path(__file__).parent
ROOT = HERE.parents[1]

for line in open(ROOT / ".env"):
    if "=" in line and not line.lstrip().startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

WS, LH = os.environ["FABRIC_SPARK_LAKEHOUSE"].split("/")
ABFSS = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH}/Files"
DFS = f"https://onelake.dfs.fabric.microsoft.com/{WS}/{LH}/Files"
API = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}"

# name: job file, local data dir -> uploaded to Files/raw/<subfolder>/, job arguments, result table to show,
# pip packages the driver needs. Fabric's own table format is Delta.
JOBS = {
    "simple":       dict(job="jobs/simple/simple.py", args=["--table", "dbo.simple_demo", "--format", "delta"],
                         result="dbo.simple_demo"),
    "etl":          dict(job="jobs/etl/etl.py", data="jobs/etl/data",
                         args=["--raw", f"{ABFSS}/raw", "--schema", "spark_demo", "--format", "delta"],
                         result="spark_demo.etl_agg_daily_country"),
    "etl_check":    dict(job="jobs/etl/check.py",
                         args=["--expected", f"{ABFSS}/raw/expected/expected.json", "--schema", "spark_demo"]),
    "coffee_gen":   dict(job="jobs/coffee/coffee_gen.py", data="jobs/coffee/data",
                         args=["--raw", f"{ABFSS}/raw/coffee_dims", "--schema", "coffee", "--format", "delta"]),
    "coffee_bench": dict(job="jobs/coffee/coffee_bench.py",
                         args=["--sql", f"{ABFSS}/raw/coffee_sql/queries.sql", "--schema", "coffee",
                               "--format", "delta"],
                         result="coffee.timings_spark"),
    # tpchgen-cli writes to a filesystem path: @MOUNT@ is Files/ mounted on the driver (an SJD has no
    # /lakehouse/default mount, unlike a notebook: files written there stay on the driver's disk and are lost)
    "tpch_gen":     dict(job="jobs/tpch/tpch_gen.py", pip=["tpchgen-cli"], args=["--raw", "@MOUNT@/raw/sf1"]),
    "tpch":         dict(job="jobs/tpch/tpch.py", data="jobs/tpch/data",
                         args=["--raw", f"{ABFSS}/raw/sf1", "--sql", f"{ABFSS}/raw/tpch_sql/tpch.sql",
                               "--schema", "tpch_sf1", "--format", "delta"],
                         result="tpch_sf1.timings"),
}

# The SJD's main file: the job's source wrapped so its output comes back. The job itself runs unchanged.
WRAPPER = '''\
import io, logging, os, subprocess, sys, sysconfig, time
from pyspark.sql import SparkSession

NAME, SRC, RESULT, PIP, LOG, FILES = {name!r}, {src!r}, {result!r}, {pip!r}, {log!r}, {files!r}

buf = io.StringIO()
class Tee:
    def __init__(self, *streams): self.streams = streams
    def write(self, s):
        for t in self.streams: t.write(s)
    def flush(self):
        for t in self.streams: t.flush()
sys.stdout = Tee(sys.__stdout__, buf)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout, force=True)
logging.getLogger("py4j").setLevel(logging.WARNING)

spark = SparkSession.builder.getOrCreate()
# Session conf, as in the other runners. Fabric runs Spark 4, where ANSI mode is on by default; the jobs are Spark 3.5 code.
for k, v in {{"spark.sql.session.timeZone": "UTC", "spark.sql.ansi.enabled": "false"}}.items():
    spark.conf.set(k, v)
print(f"spark.version={{spark.version}} | SELECT version()={{spark.sql('SELECT version()').first()[0]}}")

if PIP:
    p = subprocess.run([sys.executable, "-m", "pip", "install", "-q", *PIP], capture_output=True, text=True)
    print(f"pip install {{' '.join(PIP)}}: exit {{p.returncode}} {{(p.stdout + p.stderr)[-800:]}}")
    os.environ["PATH"] += os.pathsep + os.pathsep.join(
        sysconfig.get_path("scripts", s) for s in (sysconfig.get_default_scheme(), f"{{os.name}}_user"))

if any("@MOUNT@" in a for a in sys.argv):     # mount Files/ and hand the job its local path
    import notebookutils
    notebookutils.fs.mount(FILES, "/files")
    mnt = notebookutils.fs.getMountPath("/files")
    sys.argv = [a.replace("@MOUNT@", mnt) for a in sys.argv]
    print(f"mounted {{FILES}} at {{mnt}}")

t0, status = time.time(), "DONE"
try:
    exec(compile(SRC, NAME, "exec"), {{"__name__": "__main__", "__file__": NAME}})
except SystemExit as e:
    status = f"EXIT {{e.code}}" if e.code else "DONE"
except Exception as e:
    status = f"FAILED {{type(e).__name__}}: {{' '.join(str(e).split())[:1500]}}"
print(f"\\njob {{NAME}}: {{status}} in {{time.time() - t0:.1f}}s")
if RESULT and status == "DONE":
    spark.table(RESULT).show(50, truncate=False)

spark.createDataFrame([(buf.getvalue(),)], "value string").coalesce(1).write.mode("overwrite").text(LOG)
if status != "DONE":
    raise SystemExit(1)
'''

p = argparse.ArgumentParser()
p.add_argument("name", help=f"one of {', '.join(JOBS)}, or a path to any .py job")
p.add_argument("--args", nargs=argparse.REMAINDER, default=[], help="extra arguments passed to the job")
a = p.parse_args()
cfg = JOBS[a.name] if a.name in JOBS else dict(job=str(Path(a.name).resolve()))
job = ROOT / cfg["job"]                        # an absolute job path stays as is
name = a.name if a.name in JOBS else job.stem
job_args = cfg.get("args", []) + a.args

cred = AzureCliCredential()
api = requests.Session()
api.headers["Authorization"] = "Bearer " + cred.get_token("https://api.fabric.microsoft.com/.default").token
dfs = requests.Session()
dfs.headers.update({"Authorization": "Bearer " + cred.get_token("https://storage.azure.com/.default").token,
                    "x-ms-version": "2023-11-03"})


def check(r):
    if not r.ok:
        raise SystemExit(f"{r.request.method} {r.url} -> {r.status_code} {r.text[:1500]}")
    return r


# ---------------- OneLake Files (ADLS Gen2 DFS REST) ----------------
def put(path, data):
    url = f"{DFS}/{path}"
    check(dfs.put(url, params={"resource": "file"}))
    if data:
        check(dfs.patch(url, params={"action": "append", "position": 0}, data=data))
    check(dfs.patch(url, params={"action": "flush", "position": len(data)}))


def rmdir(path):
    r = dfs.delete(f"{DFS}/{path}", params={"recursive": "true"})
    if r.status_code != 404:
        check(r)


def ls(path):
    r = dfs.get(f"https://onelake.dfs.fabric.microsoft.com/{WS}",
                params={"resource": "filesystem", "recursive": "false", "directory": f"{LH}/Files/{path}"})
    return [] if r.status_code == 404 else [x["name"] for x in check(r).json().get("paths", [])]


if cfg.get("data"):
    for sub in sorted(d for d in (ROOT / cfg["data"]).iterdir() if d.is_dir() and not d.name.startswith("sf")):
        rmdir(f"raw/{sub.name}")               # sf*/ is generated data: tpch_gen writes it on Fabric itself
        files = sorted(f for f in sub.rglob("*") if f.is_file())
        for f in files:
            put(f"raw/{sub.name}/{f.relative_to(sub).as_posix()}", f.read_bytes())
        print(f"uploaded Files/raw/{sub.name}/: {len(files)} files")

log_dir = f"logs/{name}"
rmdir(log_dir)                                 # no stale log from an earlier run
main = WRAPPER.format(name=job.name, src=job.read_text(encoding="utf-8"), result=cfg.get("result"), pip=cfg.get("pip", []),
                      log=f"{ABFSS}/{log_dir}", files=ABFSS)
put(f"py/{name}.py", main.encode())


# ---------------- Spark Job Definition (Fabric REST) ----------------
def wait_lro(r):
    """Fabric long-running operation: 200/201 is done, 202 means poll Location until it succeeds."""
    check(r)
    while r.status_code == 202:
        time.sleep(int(r.headers.get("Retry-After", 5)))
        r = check(api.get(r.headers["Location"]))
        if r.status_code == 200 and r.json().get("status") in ("Succeeded", "Failed", "Undefined"):
            if r.json()["status"] != "Succeeded":
                raise SystemExit(f"operation failed: {r.text[:1500]}")
            break
    return r


definition = {"format": "SparkJobDefinitionV1", "parts": [{
    "path": "SparkJobDefinitionV1.json", "payloadType": "InlineBase64",
    "payload": base64.b64encode(json.dumps({
        "executableFile": f"{ABFSS}/py/{name}.py",
        "defaultLakehouseArtifactId": LH,
        "mainClass": "",
        "additionalLakehouseIds": [],
        "retryPolicy": None,
        "commandLineArguments": " ".join(job_args),
        "additionalLibraryUris": [],
        "language": "Python",
        "environmentArtifactId": None,
    }).encode()).decode()}]}

sjd_name = f"quack_{name}"
sjd = next((x for x in check(api.get(f"{API}/sparkJobDefinitions")).json()["value"]
            if x["displayName"] == sjd_name), None)
if sjd:
    wait_lro(api.post(f"{API}/sparkJobDefinitions/{sjd['id']}/updateDefinition", json={"definition": definition}))
else:
    wait_lro(api.post(f"{API}/sparkJobDefinitions", json={"displayName": sjd_name, "definition": definition}))
    sjd = next(x for x in check(api.get(f"{API}/sparkJobDefinitions")).json()["value"] if x["displayName"] == sjd_name)
print(f"SJD {sjd_name} ({sjd['id']}): {' '.join(job_args)}")

t0 = time.time()
r = check(api.post(f"{API}/items/{sjd['id']}/jobs/instances", params={"jobType": "sparkjob"}))
run_url = r.headers["Location"]
state = None
while True:
    time.sleep(10)
    run = check(api.get(run_url)).json()
    if run["status"] != state:
        state = run["status"]
        print(f"  {time.time() - t0:5.0f}s {state}")
    if state in ("Completed", "Failed", "Cancelled", "Deduped"):
        break
print(f"SJD run {state} in {time.time() - t0:.1f}s" + (f": {run['failureReason']}" if run.get("failureReason") else ""))

# The job's output: prints, logging and the result table, written by the wrapper
parts = [p for p in ls(log_dir) if Path(p).name.startswith("part-")]
if not parts:
    print("no job output written (the wrapper didn't get that far)")
for part in parts:
    print(check(dfs.get(f"https://onelake.dfs.fabric.microsoft.com/{WS}/{part}")).text)
