"""Snowflake connection: service user + PAT from the repo-root .env (gitignored; see .env.example)."""
import os
from pathlib import Path
import snowflake.connector


def connect():
    for line in open(Path(__file__).resolve().parents[2] / ".env"):
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],   # a PAT goes in the password slot
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="TPCH",
        schema="PUBLIC",
    )
