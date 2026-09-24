"""TPC-H on Spark-on-Snowflake, end to end inside the job, pure PySpark / Spark SQL (see CLAUDE.md).

0. CREATE SCHEMA + USE it (without USE, Snowpark Connect sends ~17 CURRENT_DATABASE/SCHEMA round
   trips per query -- probe/context.py). Tables already loaded with the right row count are skipped
   (idempotent; --force regenerates everything).
1. Check the stage mount (code_bundle.yml stage_mounts): exists? readable? writable?
2. For each missing table: generate with tpchgen-cli (PyPI dependency) one part at a time into /tmp,
   copy the part to the stage (mount if writable; else the injected Snowpark session's file.put;
   else Spark), delete it from /tmp. /tmp is 4 GiB, so parts stay small.
3. Load each missing table from the stage into Iceberg in TPCH.TPCH_SF<n>.
4. Run the 22 queries (lakehouse_benchmark's sql/tpch.sql) with spark.sql(...).collect().
5. Timings -> TPCH.TPCH_SF<n>.TIMINGS / LOAD_TIMINGS (Iceberg).
"""
import logging, math, re, shutil, subprocess, sys, time
from pathlib import Path
from pyspark.sql import SparkSession

log = logging.getLogger("quack.tpch")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()

SF = int(sys.argv[sys.argv.index("--sf") + 1]) if "--sf" in sys.argv else 1
FORCE = "--force" in sys.argv
S = f"TPCH.TPCH_SF{SF}"
STAGE = "@TPCH.PUBLIC.TPCH_RAW"
GEN_STAGE = f"{STAGE}/gen/sf{SF}"             # generated parquet lands in <GEN_STAGE>_<method>/<table>/
MOUNT = Path("/tmp/mnt/tpch_raw")              # stage_mounts: @TPCH.PUBLIC.TPCH_RAW (read-only on warehouses)
TMP = Path("/tmp/tpch")
TABLES = ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")
MIB_PER_SF = {"lineitem": 221, "orders": 61, "partsupp": 43, "customer": 14, "part": 7,
              "supplier": 1, "nation": 1, "region": 1}   # measured at SF1
PART_MIB = 128                                 # many small files: one 221 MiB file loaded in 174 s vs 16 x 14 MiB in 14 s
# TPC-H cardinalities; lineitem varies slightly by SF (6,001,215 at SF1, 600,037,902 at SF100)
EXPECTED = {"orders": 1_500_000 * SF, "partsupp": 800_000 * SF, "part": 200_000 * SF, "customer": 150_000 * SF,
            "supplier": 10_000 * SF, "nation": 25, "region": 5, "lineitem": 6_000_000 * SF}


def err(e):
    return f"{type(e).__name__}: {e}".splitlines()[0][:300]


# ---- 0. schema, context, what is already loaded ----
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}")
spark.sql(f"USE {S}")                          # kills the CURRENT_DATABASE/SCHEMA chatter


def loaded(t):
    """Row count if the Iceberg table exists with a plausible TPC-H count, else None."""
    if FORCE or not spark.catalog.tableExists(f"{S}.{t}"):
        return None
    n = spark.table(f"{S}.{t}").count()
    ok = abs(n - EXPECTED[t]) <= EXPECTED[t] * 0.001 if t == "lineitem" else n == EXPECTED[t]
    return n if ok else None


have = {t: loaded(t) for t in TABLES}
todo = [t for t in TABLES if have[t] is None]
for t in TABLES:
    log.info("CHECK|%s|%s", t, f"already loaded, {have[t]:,} rows -> skip" if have[t] is not None else "missing -> build")

# ---- 1. stage mount: exists / read / write ----
mount = {"exists": MOUNT.exists()}
try:
    mount["listing"] = sorted(p.name for p in MOUNT.iterdir())[:20]
except Exception as e:
    mount["listing"] = err(e)
try:
    mount["read tpch.sql"] = f"{len((MOUNT / 'raw/tpch_sql/tpch.sql').read_text())} chars"
except Exception as e:
    mount["read tpch.sql"] = err(e)
try:
    probe = MOUNT / "gen/_write_probe.txt"
    probe.parent.mkdir(parents=True, exist_ok=True)
    probe.write_text("hello")
    mount["write"] = f"OK ({probe.read_text()!r} read back)"
    probe.unlink()
    mount_rw = True
except Exception as e:
    mount["write"] = err(e)
    mount_rw = False
for k, v in mount.items():
    log.info("MOUNT|%s|%s", k, v)

# ---- Snowpark session injected by Code Bundles (docs: get_active_session) ----
try:
    from snowflake.snowpark.context import get_active_session
    snow = get_active_session()
    log.info("SESSION|get_active_session OK|%s", type(snow).__name__)
except Exception as e:
    snow = None
    log.info("SESSION|get_active_session FAIL|%s", err(e))

# ---- 2. generate part by part in /tmp -> copy to stage -> delete ----
exe = shutil.which("tpchgen-cli") or str(Path(sys.executable).parent / "tpchgen-cli")
log.info("GEN|tpchgen-cli at %s (exists=%s)", exe, Path(exe).exists())
if not Path(exe).exists():
    raise SystemExit("tpchgen-cli not installed in the sandbox -- that is the finding")


def copy_to_stage(t, part_dir, first):
    """Returns (how the part reached the stage, stage path to load from)."""
    files = sorted(part_dir.rglob("*.parquet"))
    if mount_rw:
        dest = MOUNT / "gen" / f"sf{SF}" / t
        if first:
            shutil.rmtree(dest, ignore_errors=True)
        dest.mkdir(parents=True, exist_ok=True)
        for f in files:
            shutil.copy(f, dest / f"{part_dir.name}-{f.name}")
        return "mount copy", f"{GEN_STAGE}/{t}/"
    src = files[0].parent                      # with --part, tpchgen-cli writes into <output-dir>/<table>/
    if snow is not None:                       # Snowpark file API: uploads tpchgen's own bytes, no Spark round trip
        try:
            for f in files:                    # overwrite=True + tpchgen's fixed file names: reruns don't duplicate
                snow.file.put(str(f), f"{GEN_STAGE}_put/{t}/", auto_compress=False, overwrite=True)
            return "snowpark session.file.put", f"{GEN_STAGE}_put/{t}/"
        except Exception as e:
            log.info("COPY|%s|session.file.put FAIL|%s", t, err(e))
    for path in (f"{src}/", f"file://{src}/"):
        try:
            spark.read.parquet(path).write.mode("overwrite" if first else "append").parquet(f"{GEN_STAGE}/{t}/")
            return f"spark.read.parquet({path}) -> write.parquet(stage)", f"{GEN_STAGE}/{t}/"
        except Exception as e:
            log.info("COPY|%s|%s FAIL|%s", t, path, err(e))
    raise SystemExit(f"{t}: no pure-Spark way to move /tmp parquet to the stage -- that is the finding")


gen = {}
for t in todo:
    parts = max(1, math.ceil(SF * MIB_PER_SF[t] / PART_MIB))
    t0, gen_s, copy_s, mib, how, where = time.time(), 0.0, 0.0, 0.0, "", ""
    for part in range(1, parts + 1):
        part_dir = TMP / f"{t}-{part}"
        shutil.rmtree(part_dir, ignore_errors=True)
        g0 = time.time()
        p = subprocess.run([exe, "parquet", "-s", str(SF), "--tables", t, "--parts", str(parts), "--part", str(part),
                            "--output-dir", str(part_dir), "--no-progress"], capture_output=True, text=True)
        if p.returncode:
            raise SystemExit(f"tpchgen-cli failed for {t} part {part}: {p.stderr[-800:]}")
        gen_s += time.time() - g0
        mib += sum(f.stat().st_size for f in part_dir.rglob("*.parquet")) / 2**20
        c0 = time.time()
        how, where = copy_to_stage(t, part_dir, first=(part == 1))
        copy_s += time.time() - c0
        shutil.rmtree(part_dir)                       # free /tmp before the next part
    gen[t] = (round(gen_s, 2), round(copy_s, 2), how, where)
    log.info("GEN|%s|%d parts|%.1f MiB|gen %.2fs|copy %.2fs|%s", t, parts, mib, gen_s, copy_s, how)

# ---- 3. stage -> Iceberg (only what was just generated) ----
load_rows = []
for t in todo:
    t0 = time.time()
    spark.read.parquet(gen[t][3]).write.format("iceberg").mode("overwrite").saveAsTable(f"{S}.{t}")
    n = spark.table(f"{S}.{t}").count()
    secs = round(time.time() - t0, 2)
    load_rows.append((t, n, gen[t][0], gen[t][1], secs, gen[t][2]))
    log.info("LOAD|%s|rows=%d|load %.2fs", t, n, secs)

if load_rows:                                  # keep the previous LOAD_TIMINGS when nothing was rebuilt
    (spark.createDataFrame(load_rows, "tbl string, row_count long, gen_s double, copy_s double, load_s double, copy_path string")
          .write.format("iceberg").mode("overwrite").saveAsTable(f"{S}.LOAD_TIMINGS"))

# ---- 4. the 22 queries through Spark SQL; only the timings are saved (Iceberg) ----
try:
    raw = (MOUNT / "raw/tpch_sql/tpch.sql").read_text()                # through the mount, as a plain file
except Exception as e:
    log.info("SQL|mount read failed (%s); using spark.read.text", err(e))
    raw = "\n".join(r.value for r in spark.read.text(f"{STAGE}/raw/tpch_sql/tpch.sql").collect())
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
      .write.format("iceberg").mode("overwrite").saveAsTable(f"{S}.TIMINGS"))
log.info("DONE|sf=%d|built %d tables, skipped %d|%d/22 ok|total query s=%.1f", SF, len(todo), len(TABLES) - len(todo),
         sum(r[1] == "OK" for r in results), sum(r[2] for r in results))
