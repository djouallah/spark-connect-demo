# Spark Connect demo

Plain PySpark and Spark SQL jobs, used to test what a Spark Connect platform actually supports.

- `jobs/` has the Spark code. It's pure PySpark and names no platform: paths, schema and table format are passed in as arguments.
- `data/` has the test inputs.
- `platforms/` has one folder per platform: how to deploy and run the jobs there. Snowflake is the only one so far.
- `findings/` has the results, one file per platform: [Snowflake](findings/snowflake.md).

## Run on Snowflake
```
pip install snowflake-connector-python
cp .env.example .env        # fill in a service user and PAT
python platforms/snowflake/run.py simple
python platforms/snowflake/run.py etl
```
