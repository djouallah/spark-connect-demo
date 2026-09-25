# Spark on LakeSail (Sail) with the OneLake Iceberg catalog

## Setup
- **Engine:** Sail 0.7.1 (`pysail`), started in-process by `platforms/lakesail/run.py`. It's a Rust Spark Connect server with no JVM.
- **Client:** `pyspark-client` 4.2.0, the slim Spark Connect client.
- **Catalog:** Sail's native `onelake` catalog (`api="iceberg"`) on a schema-enabled Fabric lakehouse. Tables are written as Iceberg to OneLake.
- **Inputs:** local files on the laptop, passed to the jobs as ordinary paths.
- **Auth:** one Entra token for `https://storage.azure.com/`, taken from `az login`. Sail has no credential vending, so the runner passes the token twice: `bearer_token` for the catalog and `AZURE_STORAGE_TOKEN` for the files.

## Verdict so far
The DataFrame logic runs, and `simple` computes the same result as on Snowflake. **Writing tables is the blocker:** Sail can't replace a table on this catalog, and every job here overwrites its outputs.

## Table writes (`platforms/lakesail/probe_writes.py`)
| Works | Fails |
|---|---|
| `CREATE SCHEMA`, `DROP TABLE IF EXISTS` | `USE <schema>`: "expected 'DATABASE', 'SCHEMA', 'NAMESPACE', or 'CATALOG'". Spark SQL accepts the bare form. |
| `saveAsTable`, mode append (new or existing table) or errorifexists | `saveAsTable`, mode **overwrite** (new or existing table): "Replace table is not supported yet" |
| `CREATE TABLE ... USING iceberg AS SELECT` | `CREATE OR REPLACE TABLE ... AS`: same "Replace table" error |
| `writeTo(t).using("iceberg").create()` | `writeTo(t).createOrReplace()`: same "Replace table" error |
| `CREATE TABLE (...) USING iceberg` | `writeTo(t).overwrite(df.id > 0)`: "attribute id is missing from the schema" |
| `INSERT INTO`, `INSERT OVERWRITE`, `insertInto(t, overwrite=True)` | `DELETE`: "Iceberg DELETE with `write.delete.mode=copy-on-write` is not supported yet; set `write.delete.mode=merge-on-read`" |
| | `TRUNCATE TABLE`: not parsed |

## Gotchas
- **A table must name its format.** `saveAsTable` with no format defaults to parquet, and the OneLake Iceberg REST catalog refuses to create it: "Iceberg REST catalog cannot create 'parquet' tables". That's why `simple.py` now takes `--format`.
- **MERGE needs a table property.** From the dbt-fabric smoke test: Iceberg MERGE needs `write.merge.mode=merge-on-read`, and DELETE needs `write.delete.mode=merge-on-read`.
- **The catalog list is read once, at server start.** Setting `SAIL_CATALOG__LIST` after `SparkConnectServer()` starts has no effect.
- **pandas 3 isn't supported by the client yet.** `pyspark-client` 4.2.0 warns about it, so the venv pins `pandas<3`.
