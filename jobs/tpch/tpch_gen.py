"""Generate TPC-H parquet with tpchgen-cli into --raw, one folder per table (what tpch.py --raw reads).

    --raw <destination>  [--sf 1]  [--force]

Any platform with a filesystem path (local folder, a volume): tpchgen-cli writes straight into --raw.

NON-STANDARD, Snowflake only: Snowflake has no volumes, --raw is a stage (@...), and nothing but Snowpark
can put a file written by a tool onto it (findings/snowflake.md, "Files are not standard"). So when the
Snowpark session is there, parts go to /tmp one at a time (4 GiB), are copied up with session.file.put,
then deleted. A table whose --raw folder already has the TPC-H row count is skipped.
"""
import logging, math, shutil, subprocess, sys, time
from pathlib import Path
from pyspark.sql import SparkSession

log = logging.getLogger("quack.tpch_gen")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()


def arg(name, default=None):
    """Job argument `--name value`. Every platform-specific value (paths, schema, format) comes in this way."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    if default is None:
        raise SystemExit(f"missing job argument {name}")
    return default


RAW = arg("--raw").rstrip("/")
SF = int(arg("--sf", "1"))
FORCE = "--force" in sys.argv
TMP = Path("/tmp/tpch")
TABLES = ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")
MIB_PER_SF = {"lineitem": 221, "orders": 61, "partsupp": 43, "customer": 14, "part": 7,
              "supplier": 1, "nation": 1, "region": 1}   # measured at SF1
PART_MIB = 128                                 # many small files: one 221 MiB file loaded in 174 s vs 16 x 14 MiB in 14 s
EXPECTED = {"orders": 1_500_000 * SF, "partsupp": 800_000 * SF, "part": 200_000 * SF, "customer": 150_000 * SF,
            "supplier": 10_000 * SF, "nation": 25, "region": 5, "lineitem": 6_000_000 * SF}


def err(e):
    return f"{type(e).__name__}: {e}".splitlines()[0][:300]


# Which engine runs this script. Every `if ENGINE == ...` below is a difference between engines (findings/<engine>.md).
try:
    from snowflake.snowpark.context import get_active_session
    get_active_session()                         # only Snowflake Code Bundles have a Snowpark session
    ENGINE = "snowflake"
except Exception:
    # Spark SQL version() is the engine's own version: spark.version starts with it on Spark (Fabric adds a suffix), not on Sail
    ENGINE = "spark" if spark.version.startswith(spark.sql("SELECT version()").first()[0].split()[0]) else "sail"
snow = get_active_session() if ENGINE == "snowflake" else None   # NON-STANDARD: only for file.put below
log.info("ENGINE|%s", ENGINE)


def done(t):
    """True if <RAW>/<t>/ already holds the TPC-H row count."""
    if FORCE:
        return False
    try:
        n = spark.read.parquet(f"{RAW}/{t}/").count()
    except Exception:
        return False
    return abs(n - EXPECTED[t]) <= EXPECTED[t] * 0.001 if t == "lineitem" else n == EXPECTED[t]


todo = [t for t in TABLES if not done(t)]
log.info("CHECK|to generate: %s", ",".join(todo) or "nothing")

exe = shutil.which("tpchgen-cli") or str(Path(sys.executable).parent / "tpchgen-cli")
if todo and not Path(exe).exists():
    raise SystemExit("tpchgen-cli not installed (pip install tpchgen-cli)")


def copy_to_stage(t, part_dir, first):
    """Snowflake only: put one generated part on the stage. Returns how it got there."""
    files = sorted(part_dir.rglob("*.parquet"))
    try:
        for f in files:                        # overwrite=True + tpchgen's fixed file names: reruns don't duplicate
            snow.file.put(str(f), f"{RAW}/{t}/", auto_compress=False, overwrite=True)
        return "snowpark session.file.put"
    except Exception as e:
        log.info("COPY|%s|session.file.put FAIL|%s", t, err(e))
    src = files[0].parent                      # with --part, tpchgen-cli writes into <output-dir>/<table>/
    for path in (f"{src}/", f"file://{src}/"):
        try:
            spark.read.parquet(path).write.mode("overwrite" if first else "append").parquet(f"{RAW}/{t}/")
            return f"spark.read.parquet({path}) -> write.parquet(stage)"
        except Exception as e:
            log.info("COPY|%s|%s FAIL|%s", t, path, err(e))
    raise SystemExit(f"{t}: no way to move /tmp parquet to the stage")


for t in todo:
    parts = max(1, math.ceil(SF * MIB_PER_SF[t] / PART_MIB))
    gen_s, copy_s, how = 0.0, 0.0, "tpchgen-cli -> --raw"
    if ENGINE != "snowflake":
        shutil.rmtree(Path(RAW) / t, ignore_errors=True)
    for part in range(1, parts + 1):
        out = TMP / f"{t}-{part}" if ENGINE == "snowflake" else Path(RAW)
        if ENGINE == "snowflake":
            shutil.rmtree(out, ignore_errors=True)
        g0 = time.time()
        p = subprocess.run([exe, "parquet", "-s", str(SF), "--tables", t, "--parts", str(parts), "--part", str(part),
                            "--output-dir", str(out), "--no-progress"], capture_output=True, text=True)
        if p.returncode:
            raise SystemExit(f"tpchgen-cli failed for {t} part {part}: {p.stderr[-800:]}")
        gen_s += time.time() - g0
        if ENGINE == "snowflake":
            c0 = time.time()
            how = copy_to_stage(t, out, first=(part == 1))
            copy_s += time.time() - c0
            shutil.rmtree(out)                 # free /tmp before the next part
    log.info("GEN|%s|%d parts|gen %.2fs|copy %.2fs|%s", t, parts, gen_s, copy_s, how)

log.info("DONE|sf=%d|generated %d tables into %s", SF, len(todo), RAW)
