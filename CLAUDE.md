# Spark Connect demo

## INVARIANT: job code is pure PySpark / Spark SQL
The whole point of this repo is to test what Spark supports on each platform.
- Jobs in `jobs/` use only the PySpark DataFrame API and `spark.sql()` with Spark SQL syntax.
- **Never** use platform-specific SQL or APIs in a job: no `SnowflakeSession`, no Snowpark, no Snowflake-only syntax, no passthrough of any kind.
- Jobs name no platform: stage paths, schema names and table format come in as job arguments (`--raw`, `--schema`, `--format`, ...).
- If a Spark API or Spark SQL statement fails, that failure **is the finding**. Log it, report it and record it in `findings/<platform>.md`. Do not work around it with platform SQL.
- Only tooling under `platforms/<platform>/` may use platform SQL or APIs: uploading, submitting, data prep (`tpch_gen.py`) and read-only check scripts.

## Layout
- `jobs/<test>/`: one folder per test (`simple`, `etl`, `coffee`, `dml`, `tpch`) with its portable Spark job(s) and its `data/`.
- `platforms/snowflake/`: `run.py` holds every Snowflake-specific value in `JOBS`. Also the bundle specs, `sf.py`, `check_etl.py`, the sandbox probes and `tpch_gen.py`.
- `findings/`: the results, one file per platform.
