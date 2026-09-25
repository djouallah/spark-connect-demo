# Spark on LakeSail (Sail) with the OneLake Iceberg catalog

## Setup
- **Engine:** Sail 0.7.1 (`pysail`), started in-process by `platforms/lakesail/run.py`. It's a Rust Spark Connect server with no JVM.
- **Client:** `pyspark-client` 4.2.0, the slim Spark Connect client.
- **Catalog:** Sail's native `onelake` catalog (`api="iceberg"`) on a schema-enabled Fabric lakehouse. Tables are written as Iceberg to OneLake.
- **Inputs:** local files on the laptop, passed to the jobs as ordinary paths.
- **Auth:** one Entra token for `https://storage.azure.com/`, taken from `az login`. Sail has no credential vending, so the runner passes the token twice: `bearer_token` for the catalog and `AZURE_STORAGE_TOKEN` for the files.

## Verdict so far
**`simple` and `etl` pass, with the same answers as on Snowflake.** `etl_check` gets ALL PASS: the Python UDF, the pandas UDF, windows and a `days()`-partitioned Iceberg table all behave correctly. `coffee`, `dml` and `tpch` haven't been run yet.

Getting there needed one non-standard branch. **Sail can't replace a table on this catalog**, and every job overwrites its outputs. So each job does `if ENGINE == "sail":` drop the table, then create it, `else:` overwrite it, right at each write and marked `NON-STANDARD`.

**How a job knows its engine:** a Snowpark session means Snowflake. Otherwise, Spark SQL `version()` returns Sail's own version (`0.7.1`) on Sail, while on Spark it matches `spark.version` (Snowflake returns `3.5.6` for both).

## Table writes (probed on Sail 0.7.1)
| Works | Fails |
|---|---|
| `CREATE SCHEMA`, `DROP TABLE IF EXISTS` | `USE <schema>`: "expected 'DATABASE', 'SCHEMA', 'NAMESPACE', or 'CATALOG'". Spark SQL accepts the bare form. |
| `saveAsTable`, mode append (new or existing table) or errorifexists | `saveAsTable`, mode **overwrite** (new or existing table): "Replace table is not supported yet" |
| `CREATE TABLE ... USING iceberg AS SELECT` | `CREATE OR REPLACE TABLE ... AS`: same "Replace table" error |
| `writeTo(t).using("iceberg").create()` | `writeTo(t).createOrReplace()`: same "Replace table" error |
| `CREATE TABLE (...) USING iceberg`, including `TBLPROPERTIES (...)` | `writeTo(t).overwrite(df.id > 0)`: "attribute id is missing from the schema" |
| `INSERT INTO`, `INSERT OVERWRITE`, `insertInto(t, overwrite=True)` | `DELETE`: "Iceberg DELETE with `write.delete.mode=copy-on-write` is not supported yet; set `write.delete.mode=merge-on-read`" |
| `USE SCHEMA <schema>` (the jobs use this form now; Snowflake accepts it too) | `TRUNCATE TABLE`: not parsed |
| | `ALTER TABLE ... SET TBLPROPERTIES`: "ALTER TABLE is not yet supported for catalog-managed Iceberg tables" |
| | `DELETE ... WHERE id = 1` on a bigint column: "Invalid comparison operation: Int64 == Int32". The literal isn't coerced. |

## Gotchas
- **ANSI mode is on, as in Spark 4.** A bad `to_timestamp()` or `cast` raises an error ("expected numeric datetime field", "Cannot cast string 'x'") instead of returning NULL. The jobs are Spark 3.5 code, which is what Snowflake runs, so the runner sets `spark.sql.ansi.enabled=false` and `spark.sql.session.timeZone=UTC`, like the `spark_conf` in the Snowflake bundles. `try_to_timestamp` also works.
- **A table must name its format.** `saveAsTable` with no format defaults to parquet, and the OneLake Iceberg REST catalog refuses to create it: "Iceberg REST catalog cannot create 'parquet' tables". That's why `simple.py` now takes `--format`.
- **DELETE, UPDATE and MERGE need a table property.** Iceberg MERGE needs `write.merge.mode=merge-on-read` and DELETE needs `write.delete.mode=merge-on-read`. It must be set when the table is created, because `ALTER TABLE` can't set it. `dml.py`'s setup does this on Sail.
- **The catalog list is read once, at server start.** Setting `SAIL_CATALOG__LIST` after `SparkConnectServer()` starts has no effect.
- **pandas 3 isn't supported by the client yet.** `pyspark-client` 4.2.0 warns about it, so the venv pins `pandas<3`.
