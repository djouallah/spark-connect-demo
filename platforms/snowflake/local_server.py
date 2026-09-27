"""Snowflake's Spark Connect server on the laptop: snowpark-connect runs it locally (with a JVM) and executes
the plans on a Snowflake warehouse. Any Spark Connect client can then use sc://localhost:15002, e.g. local.ipynb.
Everything lives in the repo: the venv, and the JRE from the [jdk] extra inside it.

    python -m venv .venv-snowflake
    .venv-snowflake/Scripts/python -m pip install --no-cache-dir "snowpark-connect[jdk]"
    .venv-snowflake/Scripts/python platforms/snowflake/local_server.py

Uses the same .env keys as sf.py (a PAT in the password slot).
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PORT = 15002

for line in open(ROOT / ".env"):
    if "=" in line and not line.lstrip().startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# jdk4py ships the JRE inside the venv; point the server at it instead of a system Java. Its bin/ must also be on
# PATH on Windows: the in-process JVM's jimage.dll finds zip.dll by DLL search, and without it the JVM crashes.
import jdk4py
os.environ["JAVA_HOME"] = str(jdk4py.JAVA_HOME)
os.environ["PATH"] = str(jdk4py.JAVA_HOME / "bin") + os.pathsep + os.environ["PATH"]

from snowflake import snowpark_connect

print(f"snowpark-connect server on sc://localhost:{PORT}", flush=True)
snowpark_connect.start_session(
    is_daemon=False,
    tcp_port=PORT,
    connection_parameters=dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PAT"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="TPCH",
        schema="PUBLIC",
    ),
    app_name="spark_connect_demo_local",
)
