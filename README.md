# Spark Connect demo: PySpark on Snowflake Code Bundles

Pure PySpark and Spark SQL jobs run on Snowflake as Code Bundles, to see what Spark supports there.
No Snowflake SQL runs inside a job. When Spark fails, that failure is the finding.

**Findings: [review.md](review.md)**

## Run
1. `pip install snowflake-connector-python`
2. Copy `.env.example` to `.env` and fill in a service user and PAT.
3. Submit a job. Its `code_bundle.yml` sits next to it:
   ```
   python run.py simple/job.py --result CODE_BUNDLE_DEMO
   python run.py etl/etl.py --result ETL_AGG_DAILY_COUNTRY --data-dir etl/data
   ```

| Folder | What it tests |
|---|---|
| `simple/` | Smoke test |
| `etl/` | Messy stage files to Iceberg, with UDFs and a pandas UDF; `check.py` verifies the output |
| `coffee/` | Third-party generator and its 17 benchmark queries, unchanged |
| `dml/` | DML/DDL matrix on Iceberg and native tables |
| `probe/` | Sandbox hardware, network, packages, stage mounts |
| `tpch/` | TPC-H generated inside the job and loaded to Iceberg |
