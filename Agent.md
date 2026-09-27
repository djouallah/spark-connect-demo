# Spark Connect demo

## INVARIANT: job code is pure PySpark / Spark SQL
The whole point of this repo is to test what Spark supports on each platform.
- Jobs in `jobs/` use only the PySpark DataFrame API and `spark.sql()` with Spark SQL syntax.
- **Never** use platform-specific SQL or APIs in a job: no `SnowflakeSession`, no Snowpark, no Snowflake-only syntax, no passthrough of any kind.
- Jobs name no platform: stage paths, schema names and table format come in as job arguments (`--raw`, `--schema`, `--format`, ...).
- If a Spark API or Spark SQL statement fails, that failure **is the finding**. Log it, report it and record it in `findings/<platform>.md`. Do not work around it with platform SQL.
- Engine differences are the point of the repo. Each job detects `ENGINE` (`snowflake`, `sail` or `spark`) at the top. Where an engine needs something different, write it inline at that spot as `if ENGINE == "<engine>": ... else: <standard Spark>`, marked `NON-STANDARD`, and record it in `findings/<engine>.md`. Don't add helper modules, per-engine files or abstractions: each job stays one plain Spark script.
- Tooling under `platforms/<platform>/` may use platform SQL or APIs: uploading and submitting.

## INVARIANT: a lakehouse test, open table formats only
Every table a job writes is an open table format (Iceberg, or Delta on Fabric), passed as `--format` (default `iceberg`). Never write, test or score a platform's proprietary table format ("native" tables). The results compare engines on open tables, not table formats.

## Layout
- `jobs/<test>/`: one folder per test (`simple`, `etl`, `coffee`, `dml`, `tpch`) with its portable Spark job(s) and its `data/`.
- `platforms/<platform>/`: `run.py` holds every value specific to that platform in `JOBS` (Snowflake: stages and bundle specs; LakeSail: a local Sail server with the OneLake Iceberg catalog; Fabric: a Spark Job Definition submit through the Fabric REST API, not Spark Connect, and its `ENGINE` is `spark`).
- `platforms/snowflake/`: Also the bundle specs, `sf.py`, the sandbox probes and `local_server.py` (Snowflake's Spark Connect server on the laptop, `sc://localhost:15002`, from `.venv-snowflake`).
- `spark_connect.ipynb`: 44 pure-Spark checks (catalog, DataFrame, SQL, UDFs, local files, Iceberg writes) run from the laptop against `ENDPOINT` = `sail` (in-process, OneLake), `snowflake` (`local_server.py`, started if needed), `fabric` (a Livy session, not Spark Connect: every cell not starting with `# laptop` is sent to it unchanged) or any `sc://` URL. Kernel: `.venv-snowflake` (PySpark 3.5.6 client).
- `findings/`: the results, one file per platform.
