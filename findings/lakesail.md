# Spark on LakeSail (Sail) with the OneLake Iceberg catalog

## Setup
- **Engine:** Sail 0.7.1 (`pysail`), started in-process by `platforms/lakesail/run.py`. It's a Rust Spark Connect server with no JVM.
- **Client:** `pyspark-client` 4.2.0, the slim Spark Connect client.
- **Catalog:** Sail's native `onelake` catalog (`api="iceberg"`) on a schema-enabled Fabric lakehouse. Tables are written as Iceberg to OneLake.
- **Inputs:** local files on the laptop, passed to the jobs as ordinary paths.
- **Auth:** one Entra token for `https://storage.azure.com/`, taken from `az login`. Sail has no credential vending, so the runner passes the token twice: `bearer_token` for the catalog and `AZURE_STORAGE_TOKEN` for the files.

## Verdict so far
**`simple` and `etl` pass, with the same answers as on Snowflake.** `etl_check` gets ALL PASS: the Python UDF, the pandas UDF, windows and a date-partitioned Iceberg table all behave correctly.

Getting there needed one non-standard branch. **Sail can't replace a table on this catalog**, and every job overwrites its outputs. So each job does `if ENGINE == "sail":` drop the table, then create it, `else:` overwrite it, right at each write and marked `NON-STANDARD`.

**How a job knows its engine:** a Snowpark session means Snowflake. Otherwise, Spark SQL `version()` returns Sail's own version (`0.7.1`) on Sail, while on Spark it matches `spark.version` (Snowflake returns `3.5.6` for both).

**`coffee_gen` runs on Sail, but the final write didn't finish from the laptop.** All the naming checks pass (`USE SCHEMA`, `setCurrentDatabase`, 1-, 2- and 3-part names, `listTables`). At `--orders 1000000`, the generator built `FACTSBASE` with 1,407,945 rows (`rand` seeds, `crossJoin`, `sequence`/`posexplode`, `sha2`, `create_map`). The final `FACT_SALES` write failed on the network: Sail uploads the file to OneLake in one PUT, and from a laptop to North Europe that PUT ran past Sail's 180 s retry limit ("Error performing PUT ... after 5 retries ... retry_timeout: 180s"). Run it near the lakehouse (a Fabric notebook) to get a result. Note that below about 0.5 orders per store per day (under about 400k orders), the generator rounds every day to 0 orders on any engine, so the table is empty.

**TPC-H SF1: generation works, but the load didn't finish from the laptop.** `tpch_gen.py` took its standard branch: `tpchgen-cli` wrote SF1 straight into a local folder (8 tables, 346 MB, 16 s). `tpch.py` then failed on its first table: Sail's PUT of the `lineitem` parquet to OneLake gave up after 10 retries (156 s). It's the same laptop-to-North-Europe upload limit as `coffee_gen`, so the 22 queries haven't run on Sail yet. They need to run near the lakehouse.

## DML / DDL matrix (`jobs/dml/dml.py`, Iceberg on OneLake)
**14 of 36 Iceberg checks pass, 1 is WRONG and 21 fail.** Snowflake passed 43 of 59, counting its native tables. The 23 native checks all fail here because this catalog only creates Iceberg tables ("cannot create 'parquet' tables"), so they don't apply.

| Result | Checks |
|---|---|
| PASS | INSERT VALUES / SELECT, INSERT OVERWRITE, MERGE upsert (`UPDATE SET *` / `INSERT *`), MERGE with conditional DELETE, **MERGE `WHEN NOT MATCHED BY SOURCE` DELETE and UPDATE (these fail on Snowflake)**, append `saveAsTable`, `insertInto`, `insertInto` overwrite, `writeTo.append`, `CREATE TABLE ... PARTITIONED BY (days())`, append with `mergeSchema`, DROP TABLE |
| **WRONG** | `INSERT OVERWRITE` with `partitionOverwriteMode=dynamic`: it got `[(22, 220.0)]` but should have got `[(1, 10.0), (22, 220.0)]`. **It silently deleted a partition it shouldn't have touched.** |
| FAIL | `UPDATE`: `CommandNode::Update` is unsupported |
| FAIL | `DELETE WHERE`: "Iceberg equality delete column 'amount' has identifier-field-invalid type double; float and double cannot be equality delete keys" |
| FAIL | `TRUNCATE`: not parsed |
| FAIL | `writeTo.overwrite(cond)` and `writeTo.overwritePartitions()`: "predicate or partition overwrite for Iceberg" is unsupported |
| FAIL | `ALTER TABLE` ADD / RENAME / DROP COLUMN and RENAME TO: "unsupported ALTER TABLE operation" |
| FAIL | `CTAS` (`CREATE OR REPLACE`): "Replace table is not supported yet" |
| FAIL | Persistent `CREATE VIEW`: "Failed to create view ... 404 Not Found" from the catalog |
| FAIL | `.snapshots`, `.history`, time travel (`TIMESTAMP AS OF`, `as-of-timestamp`, `VERSION AS OF`), `CALL system.rollback_to_snapshot`: "Failed to load table ... 400 Bad Request" |

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
