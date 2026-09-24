# Spark on Snowflake (Code Bundles): a hands-on review

## Verdict
**It works, and much better than expected.** We ran plain PySpark and Spark SQL on Snowflake as Code Bundles. Every job:
- read raw files from stages
- transformed them with the DataFrame API, Spark SQL and Python/pandas UDFs
- wrote Iceberg tables

The results were checked against independently computed answers, down to the cent. A third-party Spark generator and its 17-query benchmark ran with a single one-line change. There's a short list of real gaps (below), mostly in advanced MERGE, overwrite and Iceberg-metadata features. Scale was deliberately not the focus.

All tests are **pure PySpark and Spark SQL**, with no Snowflake SQL inside a job. When Spark fails, that failure is the finding (see `CLAUDE.md`).

## What works well (the pleasant surprises)
- **TPC-H SF100 data generated and loaded inside one Spark job on an X-Small warehouse** (`tpch/`, nothing generated on the laptop):

  | Step | How | Result |
  |---|---|---|
  | Generate | `tpchgen-cli` installed as a PyPI dependency, run with `subprocess` in the sandbox, 128 MiB parts into `/tmp` | 39.6 GB across 275 parts; about 390 s of generation |
  | Copy to stage | `get_active_session().file.put(...)`, then delete from `/tmp` | about 880 s. The 4 GB `/tmp` was never a problem. |
  | Load | `spark.read.parquet("@stage/...")` then `.write.format("iceberg").saveAsTable(...)` | 8 tables, lineitem **600,037,902 rows in 914 s**; all tables in about 26 min |

  - **File layout matters for the load.** At SF1, one 221 MiB parquet file took 174 s to load, while 16 files of 14 MiB took 14 s. Generate many ~128 MiB parts, not one big file.
- **Real third-party code runs almost unchanged** (`coffee/`).
  - Josue Bogran's coffee-shop generator runs with **one line changed**. It uses `rand` seeds, `crossJoin`, `sequence`/`posexplode`, `sha2` and `create_map`, and produced about 14.4M Iceberg rows in about 225 s.
  - **All 17 benchmark queries run unchanged through `spark.sql()`**, taking 4–11 s each. They use windows with `ROWS BETWEEN`, `RANK`/`DENSE_RANK`, CTEs, range joins and `COUNT(DISTINCT)`.
- **The ETL output is correct to the cent** (`etl/`).
  - The input was messy CSV and nested JSON on a stage.
  - The job trims every string column (driven by the schema), deduplicates, keeps the latest customer version with a window function, normalises phone numbers with a **Python UDF** and converts to USD with a **pandas UDF**.
  - It writes three Iceberg tables, one of them partitioned by day. All 15 checks against independently computed answers passed.
- **`spark.sql("CREATE OR REPLACE TABLE t USING iceberg AS SELECT ...")` creates a real Iceberg table.** The [Iceberg docs](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-iceberg) say Spark SQL DDL can't create Iceberg tables; it did. Without `USING iceberg`, the same CTAS makes a native table.
- **Everyday DML matches Spark** on both Iceberg and native tables (`dml/`, 43 of 59 checks pass, with the rows compared): INSERT with VALUES or SELECT, INSERT OVERWRITE (including dynamic partitions), DELETE, UPDATE, MERGE upsert (`UPDATE SET *` / `INSERT *`), MERGE with conditional DELETE, TRUNCATE, `insertInto`, `writeTo.append` / `.overwrite(cond)`, `CREATE TABLE ... PARTITIONED BY (days())`, `ALTER TABLE` add/rename/drop column, `ALTER TABLE ... RENAME TO`, `mergeSchema` on Iceberg, views and DROP.
- **Naming and organisation behave like a proper catalog:**
  - `CREATE SCHEMA` from Spark SQL works.
  - Three-, two- and one-part names work everywhere.
  - `USE` and `setCurrentDatabase` work.
  - A job can read from a different stage than the one its code lives in.
- **The logs show what Spark sent:** the event table contains every Spark Connect request with its plan tree. That's useful for seeing what your DataFrame code turned into.

## What you can do: patterns that work
```python
# Stage -> Spark -> Iceberg: three ways to write Iceberg, all verified
df = spark.read.option("header", True).csv("@TPCH.PUBLIC.COFFEE_RAW/raw/orders/")
df.write.format("iceberg").mode("overwrite").saveAsTable("TPCH.COFFEE.ORDERS")                    # V1
df.writeTo("TPCH.COFFEE.ORDERS_P").using("iceberg").partitionedBy(F.days("order_ts")).createOrReplace()  # V2
spark.sql("CREATE OR REPLACE TABLE TPCH.COFFEE.Q AS SELECT ...")      # native table
spark.sql("CREATE OR REPLACE TABLE TPCH.COFFEE.Q USING iceberg AS SELECT ...")  # Iceberg

# Upsert
spark.sql("""MERGE INTO TPCH.COFFEE.ORDERS t USING updates s ON t.id = s.id
             WHEN MATCHED AND s.op = 'D' THEN DELETE
             WHEN MATCHED THEN UPDATE SET *
             WHEN NOT MATCHED THEN INSERT *""")
```
- **Iceberg storage:** set `snowpark.connect.iceberg.external_volume: "SNOWFLAKE_MANAGED"` in `code_bundle.yml`, under `properties.spark_conf`.
- **Python packages for the job:** list them in `properties.python_dependencies.packages`; they're installed before the job starts. UDFs install packages separately, through `spark_conf: snowpark.connect.udf.packages: "[pandas]"`. The ETL's pandas UDF used this. See the [Code Bundle docs](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-submit-code-bundle).
- **More memory or CPU for the job's own Python** (pandas, `toPandas()`, model training): use a **Snowpark-optimized** warehouse. The job's Python got 32 vCPU / 200 GiB there, against 8 vCPU / 12 GiB on standard warehouses (see Under the hood).
- **Loading files from a laptop:** a single `PUT 'file://.../*' @stage/path/ AUTO_COMPRESS=FALSE` (see `run.py`), or drag and drop in Snowsight.
- **Reaching websites and APIs from a job:** this needs an external access integration in the spec, since the sandbox has no internet access by default ([docs](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-submit-code-bundle)). We haven't tested it yet.

## Gaps found
| Gap | What we saw | Docs | Pure-Spark alternative |
|---|---|---|---|
| MERGE `WHEN NOT MATCHED BY SOURCE` (DELETE or UPDATE) | `SnowparkConnectNotImplementedError`: "Snowflake does not support 'not matched by source'" (Iceberg and native) | Not mentioned | A separate `DELETE ... WHERE id NOT IN (SELECT id FROM src)` |
| `df.write.insertInto(t, overwrite=True)` on **Iceberg** | "Object ... already exists as ICEBERG_TABLE": it tries to recreate the table as a native table. Works on native tables. | Not mentioned | `INSERT OVERWRITE t SELECT ...` or `writeTo(t).overwrite(cond)` |
| `writeTo(t).overwritePartitions()` | Snowflake internal error, with an incident number | Not mentioned | `INSERT OVERWRITE` with `partitionOverwriteMode=dynamic` (works) |
| Iceberg metadata tables `.snapshots` / `.history` | Failed in every name form: "object name ... is invalid", or `Table '"1"' does not exist` with the extensions on. That blocks `VERSION AS OF <snapshot_id>` and `CALL system.rollback_to_snapshot`. | [Listed as supported](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-iceberg) | None found. The docs describe timestamp time travel (`as-of-timestamp`), which we didn't pursue. |
| Append with `mergeSchema` on a **native** table | Column count mismatch. Works on Iceberg. | Not mentioned | `ALTER TABLE ADD COLUMNS` first (works) |
| Spark 4-style Python code | `map_col[F.col(k)]` gives `UNSUPPORTED_DATA_TYPE`. The client is Spark **3.5.6**, so the error comes from the client, not from Snowflake. | Spark 3.5 documented | `F.element_at(map_col, F.col(k))` |

## Gotchas and behaviour differences
- **`CREATE SCHEMA` switches the session into the new schema.** That's Snowflake behaviour; Spark doesn't do it. One-part names then resolve in the new schema.
- **Unquoted identifiers come back in uppercase** (`country` becomes `COUNTRY`). Spark access stays case-insensitive, but Python `Row` attributes don't: `r.id` fails where `r["ID"]` works.
- **Timestamps land as `TIMESTAMP_LTZ`.** The account and sandbox time zone is America/Los_Angeles. Set `spark.sql.session.timeZone: UTC` and compute date columns inside the job.
- **Seeded `rand(seed)` isn't reproducible between runs:** the same seeds gave 14,420,382 rows one run and 14,419,616 the next.
- **`orderBy` before a write isn't kept** in the written table. Sort when you query.
- `catalog.listTables()` also shows internal `SNOWPARK_TEMP_TABLE_*` tables (the ones behind `createDataFrame`).
- **Logs:**
  - Only Python **`logging`** reaches the event table; `print` and `df.show()` output is lost.
  - Read from `SNOWFLAKE.TELEMETRY.EVENTS_VIEW`, which needs `GRANT APPLICATION ROLE SNOWFLAKE.EVENTS_VIEWER`.
- **Monitoring:**
  - There's no Spark UI. The [Code Bundle docs](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-submit-code-bundle) ("Monitor and manage jobs → Spark Monitoring UI") describe a Snowsight Spark run history at `#/compute/history/spark`, but it **doesn't exist in our account**.
  - What works: Query History (the live SQL each job sends), `CODE_BUNDLE_HISTORY` (RUNNING, DONE or FAILED), and the job's own `logging` lines in `EVENTS_VIEW`, which arrive while it runs.
- **Submitting jobs:**
  - `EXECUTE CODE BUNDLE FROM` must point at the **stage folder**, with the file name in `ENTRYPOINT`.
  - The call blocks until the job ends.
  - Stopping the local command does **not** stop the job; a run we aborted still created tables.
- **Set the Spark current database first, or Snowpark Connect chatters** (`probe/context.py`).
  - **The symptom:** without `spark.sql("USE db.schema")` or `spark.catalog.setCurrentDatabase(...)`, Snowpark Connect sends about 17 `SELECT CURRENT_DATABASE()` / `CURRENT_SCHEMA()` round trips per query, even with fully qualified names.
  - **The probe:** 15 queries sent 300 statements (about 23 s of chatter, 40 s wall). After `USE` or `setCurrentDatabase` it was about 33 statements, zero chatter, 7–9 s wall. Real query time was identical, about 3.5 s.
  - **At SF100, same 22 TPC-H queries on an X-Small:**
    - without `USE`: **308 s**, 1,387 statements, 1,307 of them context round trips
    - with `USE`: **157 s**, 132 statements
    - Snowflake execution was about 126–130 s both times, so the whole difference was chatter. With `USE`, Spark SQL runs TPC-H SF100 at about native speed.
  - **The fix is the explicit `USE` itself.** `USE` with fully qualified names was just as clean: 33 statements, zero chatter, 6.9 s, against 300 statements and 41 s without it. Put `spark.sql("USE <db>.<schema>")` at the top of every job.
- **Stage mounts work in Spark bundles, read-only** (`probe/mount.py`). The docs only describe them for `type: custom`.
  - A "mount" on a warehouse is a **symlink** to a read-only copy under `/home/udf/<id>/` (gVisor 9p, `ro`).
  - `mount_path: '/mnt/...'` fails at startup with `Read-only file system`, because the symlink can't be created. Put the mount under `/tmp/`, for example `/tmp/mnt/tpch_raw/`.
  - Reading works: listing, text, and a 221 MiB parquet read in 0.03 s, so the files are already local when the job starts. Every write, mkdir or delete fails with `Errno 30`, matching the [docs](https://docs.snowflake.com/en/developer-guide/code-bundles/code-bundle-yml-reference) ("read-only on warehouses").
  - Getting files *onto* a stage from a warehouse job has to go through Spark (`df.write.parquet("@stage/...")`), not the mount.
- **Iceberg SQL extensions:** setting `spark.sql.extensions` (IcebergSparkSessionExtensions) requires the package `snowpark-connect-deps-iceberg`. Across all 59 checks it made **no difference** to any result, only to some error messages.
- **Auth:** a personal user with password + MFA can't submit programmatically. Use a service user with a PAT.

## Under the hood (measured)
- **Startup:** about 30–100 s of fixed overhead per job, even for tiny data. Our guess is package install plus server start.
- **Where your Python runs:** a sandbox on one warehouse node. The traceback (`_udf_code.py`, handler `run`, `/home/udf/...`, `runpy`) suggests the bundle runs as a Python stored procedure, which is inference. Snowflake documents stored procedures as [single-node](https://docs.snowflake.com/en/developer-guide/snowpark/python/python-snowpark-training-ml).
- **Where the Spark work runs:** `SPARK_REMOTE=sc://127.0.0.1:15002`, so Spark Connect talks to a server inside the same sandbox. That server turns your plan into Snowflake SQL, which runs on the warehouse ([engineering blog](https://www.snowflake.com/en/engineering-blog/spark-connect-engine-snowflake-engineering-deep-dive/)).
- **UDFs:** Spark UDFs become Snowflake UDFs and [scale across the warehouse](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-designing). They're correct in our tests; we haven't measured their parallelism.
- **The sandbox box** (`probe/`):

  | Warehouse | vCPU | RAM | `/tmp` | CPU |
  |---|---|---|---|---|
  | Standard X-Small, Gen1 | 8 | 12 GiB | 4 GiB | ARM Neoverse N2 (`0xd49`) |
  | Standard Small, Gen1 and Gen2 | 8 | 12 GiB | 4 GiB | ARM Neoverse N2 |
  | Snowpark-optimized Medium, `MEMORY_16X` | **32** | **200 GiB** | **32 GiB** | not reported |

  - Standard sizes add nodes (which helps the SQL and UDFs), not a bigger box. The node *type* is what changes the box.
- **The sandbox itself:**
  - It reports Linux `4.4.0` with a `0/0` loadavg. That's the gVisor fingerprint, so it's a sandboxed kernel and not a VM (inference).
  - `/` is read-only.
  - There's **no internet egress**: `pypi.org` and `8.8.8.8` are blocked.
  - Python 3.11 with 87 packages: pandas, numpy, pyarrow, boto3, s3fs/gcsfs, and a JDK 21 via `jdk4py`. There's no scikit-learn, polars or duckdb.

## The tests (`python run.py <folder>/<job>.py ...`)
| Folder | What it tests | Warehouse |
|---|---|---|
| `simple/` | Smoke test: DataFrame conveniences end to end | Standard Small |
| `etl/` | Messy stage files, clean/join/UDF/pandas UDF, 3 Iceberg tables. `gen_data.py` generates the data, `check.py` verifies it. | Standard Small |
| `coffee/` | Josue Bogran's generator plus the 17 benchmark queries, run unchanged in `TPCH.COFFEE` (all Iceberg); schema, naming and stage checks | Standard Small |
| `dml/` | 59-check DML/DDL matrix, Iceberg and native, with and without the Iceberg extensions (`--spec dml/code_bundle_noext.yml`) | Standard Small |
| `probe/` | Sandbox hardware, limits, network, packages; `mount.py` checks stage-mount read/write | X-Small, Small, Snowpark-optimized Medium |
| `tpch/` | TPC-H data generation in the sandbox (`tpchgen-cli`), uploaded with `session.file.put`, loaded to Iceberg. Run with `--args --sf 100`. | Standard X-Small |

The warehouse name `XSMALL` was actually size **Small** for every run before the hardware probes. The coffee and DML timings are Small numbers.
