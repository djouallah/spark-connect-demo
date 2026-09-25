# Spark Connect demo

## INVARIANT: job code is pure PySpark / Spark SQL
The whole point of this repo is to test what Spark supports on each platform.
- Jobs in `jobs/` use only the PySpark DataFrame API and `spark.sql()` with Spark SQL syntax.
- **Never** use platform-specific SQL or APIs in a job: no `SnowflakeSession`, no Snowpark, no Snowflake-only syntax, no passthrough of any kind.
- Jobs name no platform: stage paths, schema names and table format come in as job arguments (`--raw`, `--schema`, `--format`, ...).
- If a Spark API or Spark SQL statement fails, that failure **is the finding**. Log it, report it and record it in `findings/<platform>.md`. Do not work around it with platform SQL.
- Exception: a job file named `*_<platform>.py` (e.g. `jobs/tpch/tpch_gen_snowflake.py`) is a workaround the platform forced, kept next to the test so the non-standard code is visible. Each one must be backed by a finding explaining why.
- Tooling under `platforms/<platform>/` may use platform SQL or APIs: uploading and submitting.

## Layout
- `jobs/<test>/`: one folder per test (`simple`, `etl`, `coffee`, `dml`, `tpch`) with its portable Spark job(s) and its `data/`.
- `platforms/snowflake/`: `run.py` holds every Snowflake-specific value in `JOBS`. Also the bundle specs, `sf.py` and the sandbox probes.
- `findings/`: the results, one file per platform.
