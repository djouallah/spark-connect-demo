"""What does the Code Bundle sandbox actually run on? Hardware, OS, limits, Python packages.
Pure Python + PySpark introspection; output goes to the event table via logging."""
import importlib.metadata as md, logging, os, platform, shutil, socket, sys, time

log = logging.getLogger("quack.probe")
log.setLevel(logging.INFO)


def read(path, n=2000):
    try:
        with open(path) as f:
            return f.read(n).strip()
    except Exception as e:
        return f"<{type(e).__name__}>"


def info(key, val):
    log.info("PROBE|%s|%s", key, val)


# ---- platform ----
info("python", sys.version.replace("\n", " "))
info("platform", platform.platform())
info("machine", platform.machine())
info("hostname", socket.gethostname())
info("user", f"uid={os.getuid()} gid={os.getgid()} login={os.environ.get('USER', '?')}")
info("cwd", os.getcwd())
info("argv", sys.argv)

# ---- CPU ----
info("os.cpu_count", os.cpu_count())
info("sched_getaffinity", len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else "n/a")
cpuinfo = read("/proc/cpuinfo", 20000)
models = {l.split(":", 1)[1].strip() for l in cpuinfo.splitlines() if ":" in l and l.split(":")[0].strip() in ("model name", "CPU part", "Hardware")}
info("cpu models", models or cpuinfo[:300])
info("loadavg", read("/proc/loadavg"))

# ---- memory ----
mem = {l.split(":")[0]: l.split(":")[1].strip() for l in read("/proc/meminfo", 5000).splitlines() if ":" in l}
info("meminfo", {k: mem.get(k) for k in ("MemTotal", "MemFree", "MemAvailable", "SwapTotal")})

# ---- cgroup limits (what the sandbox is actually allowed) ----
for p in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/pids.max",
          "/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
          "/sys/fs/cgroup/cpu/cpu.cfs_period_us"):
    if os.path.exists(p):
        info(f"cgroup {p}", read(p))
try:
    import resource
    info("rlimits", {n: resource.getrlimit(getattr(resource, n)) for n in ("RLIMIT_AS", "RLIMIT_NPROC", "RLIMIT_NOFILE", "RLIMIT_FSIZE")})
except Exception as e:
    info("rlimits", e)

# ---- disk / filesystem ----
for d in ("/", "/tmp", os.path.expanduser("~"), os.getcwd()):
    try:
        u = shutil.disk_usage(d)
        w = os.access(d, os.W_OK)
        info(f"disk {d}", f"total={u.total/2**30:.1f}GiB free={u.free/2**30:.1f}GiB writable={w}")
    except Exception as e:
        info(f"disk {d}", f"<{type(e).__name__}: {e}>")
t0 = time.time()
try:
    with open("/tmp/probe.bin", "wb") as f:
        f.write(os.urandom(256 * 2**20))
    os.remove("/tmp/probe.bin")
    info("write 256MiB to /tmp", f"{time.time() - t0:.2f}s")
except Exception as e:
    info("write 256MiB to /tmp", f"<{type(e).__name__}: {e}>")

# ---- network: can the sandbox reach the internet? (expect no without an external access integration) ----
for host, port in (("pypi.org", 443), ("8.8.8.8", 53)):
    try:
        socket.create_connection((host, port), timeout=3).close()
        info(f"egress {host}:{port}", "OPEN")
    except Exception as e:
        info(f"egress {host}:{port}", f"blocked ({type(e).__name__})")

# ---- tools on PATH ----
info("tools", {t: shutil.which(t) for t in ("java", "scala", "spark-submit", "pip", "uv", "conda", "git", "gcc", "duckdb")})

# ---- environment: names only (values may be credentials), plus a few safe values ----
info("env names", sorted(os.environ))
info("env values", {k: os.environ.get(k) for k in ("PATH", "HOME", "TZ", "LANG", "PYTHONPATH", "SPARK_REMOTE", "JAVA_HOME")})

# ---- Python packages ----
pkgs = sorted(f"{d.metadata['Name']}=={d.version}" for d in md.distributions())
info("package count", len(pkgs))
for i in range(0, len(pkgs), 40):
    info(f"packages {i}-{i + 39}", ", ".join(pkgs[i:i + 40]))
for name in ("pyspark", "snowpark-connect", "snowflake-snowpark-python", "pandas", "numpy", "pyarrow", "scikit-learn",
             "polars", "duckdb", "delta-spark", "pyiceberg", "grpcio", "boto3", "requests"):
    try:
        info(f"pkg {name}", md.version(name))
    except md.PackageNotFoundError:
        info(f"pkg {name}", "not installed")

# ---- Spark side ----
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
info("spark.version", spark.version)
info("spark defaultParallelism-ish", spark.conf.get("spark.sql.shuffle.partitions", "?"))
info("spark current catalog.db", f"{spark.catalog.currentCatalog()}.{spark.catalog.currentDatabase()}")
t0 = time.time()
n = spark.range(10_000_000).selectExpr("sum(id) AS s").first()["s"]
info("spark.range(10M).sum", f"{n} in {time.time() - t0:.2f}s")
log.info("DONE|probe")
