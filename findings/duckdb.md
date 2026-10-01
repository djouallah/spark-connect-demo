# DuckDB through SQLFrame (`spark_connect.ipynb`, `ENDPOINT = "duckdb"`)

Not Spark and not Spark Connect: [SQLFrame](https://github.com/eakmanrq/sqlframe) 4.4.0 gives a PySpark `SparkSession` that runs in-process on DuckDB 1.5.5, turning DataFrame code into SQL and translating Spark SQL with sqlglot. Tables are Iceberg in the same OneLake catalog as Sail, through DuckDB's iceberg extension, attached as in [djouallah/iceberg-probe-native](https://github.com/djouallah/iceberg-probe-native) (`STAGE_CREATE_TABLES false`, `SKIP_CREATE_TABLE_METADATA_UPDATES true`, `ACCESS_DELEGATION_MODE 'none'`, an `access_token` storage secret).

**24 PASS, 16 FAIL, 1 WRONG of 41** (run of 2026-10-01, per check: [`local/duckdb.csv`](local/duckdb.csv)).

- **DataFrame API and Spark SQL queries:** everything passes (`createDataFrame`, joins, `pivot`, windows, CTEs, `GROUPING SETS`, `toPandas`, ...).
- **`spark.sql()` only runs queries.** Every DDL and DML statement fails inside SQLFrame before DuckDB sees it: `CREATE SCHEMA`, `USE SCHEMA`, `SHOW NAMESPACES`, `DROP TABLE`, `DESCRIBE TABLE`, `INSERT INTO VALUES`, `MERGE`, `UPDATE`, `DELETE`, `CREATE TABLE ... PARTITIONED BY` ("Unknown expression type provided in the SQL", `'NoneType' object has no attribute 'copy'`, `'Values' object has no attribute 'ctes'`). The same statements fail the same way on a plain in-memory DuckDB, so it's SQLFrame, not OneLake.
- **Writes:** with no schema (`CREATE SCHEMA` failed), `saveAsTable` and `INSERT OVERWRITE` fail on the missing table/schema; `writeTo().append()` isn't implemented.
- **UDFs:** no `F.udf` and no `F.pandas_udf`. `spark.udf.register` + SQL works.
- **WRONG `date functions`:** `date_add(to_date(...), 30)` returns a timestamp, not a date.
- **No `spark.conf`:** the session has no `conf`, so the Connect cell sets DuckDB's `TimeZone` directly.
- DuckDB itself writes Iceberg on OneLake fine (CREATE TABLE, INSERT, UPDATE, DELETE, MERGE, DROP all work in plain DuckDB SQL); the gaps above are SQLFrame's Spark layer.
