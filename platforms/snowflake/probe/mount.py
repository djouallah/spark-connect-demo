"""Stage mount probe: does a type:spark Code Bundle accept stage_mounts, and can it read / write / delete?
Plain Python file operations only; results go to the event table via logging."""
import logging, os, time
from pathlib import Path

log = logging.getLogger("quack.mount")
log.setLevel(logging.INFO)
M = Path("/tmp/mnt/tpch_raw")


def step(name, fn):
    try:
        log.info("MOUNT|%s|OK|%s", name, fn())
    except Exception as e:
        log.info("MOUNT|%s|FAIL|%s", name, f"{type(e).__name__}: {e}".splitlines()[0][:300])


def timed_read():
    t0 = time.time()
    n = len((M / "raw/lineitem/lineitem.parquet").read_bytes())
    return f"{n / 2**20:.1f} MiB in {time.time() - t0:.2f}s"


step("exists", lambda: M.exists())
step("/proc/mounts entry", lambda: [l.strip() for l in open("/proc/mounts") if "/mnt" in l or "udf" in l][:3])
step("listdir /mnt/tpch_raw", lambda: sorted(os.listdir(M))[:20])
step("listdir raw/", lambda: sorted(os.listdir(M / "raw"))[:20])
step("read raw/tpch_sql/tpch.sql", lambda: f"{len((M / 'raw/tpch_sql/tpch.sql').read_text())} chars")
step("read raw/lineitem/lineitem.parquet fully", timed_read)
step("os.access W_OK", lambda: os.access(M, os.W_OK))
step("mkdir probe/", lambda: (M / "probe").mkdir(exist_ok=True))
step("write probe/hello.txt", lambda: (M / "probe/hello.txt").write_text("hello"))
step("read back probe/hello.txt", lambda: (M / "probe/hello.txt").read_text())
step("write probe/big.bin (64 MiB)", lambda: (M / "probe/big.bin").write_bytes(os.urandom(64 * 2**20)))
step("delete probe/hello.txt", lambda: (M / "probe/hello.txt").unlink())
log.info("DONE|mount probe")
step("mount is a symlink?", lambda: f"{M.is_symlink()} -> {os.path.realpath(M)}")
step("target filesystem writable?", lambda: os.access(os.path.realpath(M), os.W_OK))
