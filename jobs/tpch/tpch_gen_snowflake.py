"""NON-STANDARD, Snowflake only: Snowflake has no volumes, so this needs Snowpark (see findings/snowflake.md,
"Files are not standard"). On a platform with volumes this file isn't needed: run tpchgen-cli into a folder
and point tpch.py --raw at it.

Data prep for tpch.py: generate TPC-H parquet inside a Code Bundle and put it on a stage.

    --sf 1  [--stage @TPCH.PUBLIC.TPCH_RAW]  [--force]

Generates with tpchgen-cli (PyPI dependency) one part at a time into /tmp, copies each part to
<stage>/gen/sf<n>_put/<table>/ with the injected Snowpark session's file.put (Spark as fallback),
then deletes it from /tmp: /tmp is 4 GiB, so parts stay small. A table whose stage folder already
has the TPC-H row count is skipped.
"""
import logging, math, shutil, subprocess, sys, time
from pathlib import Path
from pyspark.sql import SparkSession

log = logging.getLogger("quack.tpch_gen")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()

SF = int(sys.argv[sys.argv.index("--sf") + 1]) if "--sf" in sys.argv else 1
STAGE = sys.argv[sys.argv.index("--stage") + 1] if "--stage" in sys.argv else "@TPCH.PUBLIC.TPCH_RAW"
FORCE = "--force" in sys.argv
DEST = f"{STAGE}/gen/sf{SF}_put"               # tpch.py --raw points here
TMP = Path("/tmp/tpch")
TABLES = ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")
MIB_PER_SF = {"lineitem": 221, "orders": 61, "partsupp": 43, "customer": 14, "part": 7,
              "supplier": 1, "nation": 1, "region": 1}   # measured at SF1
PART_MIB = 128                                 # many small files: one 221 MiB file loaded in 174 s vs 16 x 14 MiB in 14 s
EXPECTED = {"orders": 1_500_000 * SF, "partsupp": 800_000 * SF, "part": 200_000 * SF, "customer": 150_000 * SF,
            "supplier": 10_000 * SF, "nation": 25, "region": 5, "lineitem": 6_000_000 * SF}


def err(e):
    return f"{type(e).__name__}: {e}".splitlines()[0][:300]


def on_stage(t):
    """True if <DEST>/<t>/ already holds the TPC-H row count."""
    if FORCE:
        return False
    try:
        n = spark.read.parquet(f"{DEST}/{t}/").count()
    except Exception:
        return False
    return abs(n - EXPECTED[t]) <= EXPECTED[t] * 0.001 if t == "lineitem" else n == EXPECTED[t]


todo = [t for t in TABLES if not on_stage(t)]
log.info("CHECK|to generate: %s", ",".join(todo) or "nothing")

# ---- Snowpark session injected by Code Bundles (docs: get_active_session) ----
try:
    from snowflake.snowpark.context import get_active_session
    snow = get_active_session()
    log.info("SESSION|get_active_session OK|%s", type(snow).__name__)
except Exception as e:
    snow = None
    log.info("SESSION|get_active_session FAIL|%s", err(e))

exe = shutil.which("tpchgen-cli") or str(Path(sys.executable).parent / "tpchgen-cli")
log.info("GEN|tpchgen-cli at %s (exists=%s)", exe, Path(exe).exists())
if todo and not Path(exe).exists():
    raise SystemExit("tpchgen-cli not installed in the sandbox")


def copy_to_stage(t, part_dir, first):
    """Returns how the part reached the stage."""
    files = sorted(part_dir.rglob("*.parquet"))
    if snow is not None:                       # Snowpark file API: uploads tpchgen's own bytes, no Spark round trip
        try:
            for f in files:                    # overwrite=True + tpchgen's fixed file names: reruns don't duplicate
                snow.file.put(str(f), f"{DEST}/{t}/", auto_compress=False, overwrite=True)
            return "snowpark session.file.put"
        except Exception as e:
            log.info("COPY|%s|session.file.put FAIL|%s", t, err(e))
    src = files[0].parent                      # with --part, tpchgen-cli writes into <output-dir>/<table>/
    for path in (f"{src}/", f"file://{src}/"):
        try:
            spark.read.parquet(path).write.mode("overwrite" if first else "append").parquet(f"{DEST}/{t}/")
            return f"spark.read.parquet({path}) -> write.parquet(stage)"
        except Exception as e:
            log.info("COPY|%s|%s FAIL|%s", t, path, err(e))
    raise SystemExit(f"{t}: no way to move /tmp parquet to the stage")


for t in todo:
    parts = max(1, math.ceil(SF * MIB_PER_SF[t] / PART_MIB))
    gen_s, copy_s, mib, how = 0.0, 0.0, 0.0, ""
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
        how = copy_to_stage(t, part_dir, first=(part == 1))
        copy_s += time.time() - c0
        shutil.rmtree(part_dir)                       # free /tmp before the next part
    log.info("GEN|%s|%d parts|%.1f MiB|gen %.2fs|copy %.2fs|%s", t, parts, mib, gen_s, copy_s, how)

log.info("DONE|sf=%d|generated %d tables into %s", SF, len(todo), DEST)
