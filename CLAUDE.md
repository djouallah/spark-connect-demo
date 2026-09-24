# quack: Spark on Snowflake (Code Bundles) tests

## INVARIANT: job code is pure PySpark / Spark SQL
The whole point of this repo is to test what Spark supports on Snowflake.
- Job files (`*/*.py` submitted with `run.py`) use only the PySpark DataFrame API and `spark.sql()` with Spark SQL syntax.
- **Never** use Snowflake SQL in a job: no `SnowflakeSession`, no Snowflake-only syntax, no passthrough of any kind.
- If a Spark API or Spark SQL statement fails, that failure **is the finding**. Log it, report it and record it in `learning.md`. Do not work around it with Snowflake SQL.
- Only local tooling may talk Snowflake SQL through the connector: `run.py` (upload and submit), `sf.py`, and read-only check scripts.

## Layout
One folder per test (`simple/`, `etl/`, `coffee/`). Each has its job(s) and a `code_bundle.yml`. Findings go in `learning.md`.
