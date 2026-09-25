"""Which table-write forms does Sail accept on the OneLake Iceberg REST catalog? One line per form, OK or FAIL.

    python platforms/lakesail/run.py platforms/lakesail/probe_writes.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
S = "probe_w"
df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string")


def p(name, fn):
    try:
        r = fn()
        print(f"OK    {name}  {r if r is not None else ''}")
    except Exception as e:
        print(f"FAIL  {name}  {type(e).__name__}: {' '.join(str(e).split())[:220]}")


def n(t):
    return spark.table(t).count()


p("CREATE SCHEMA", lambda: spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}").collect())
p("USE", lambda: spark.sql(f"USE {S}").collect())
for t in ("t_ow", "t_app", "t_err", "t_ctas", "t_corctas", "t_v2c", "t_v2cor", "t_ddl"):
    p(f"DROP TABLE IF EXISTS {t}", lambda t=t: spark.sql(f"DROP TABLE IF EXISTS {S}.{t}").collect())
p("saveAsTable overwrite (new)", lambda: df.write.format("iceberg").mode("overwrite").saveAsTable(f"{S}.t_ow"))
p("saveAsTable append (new)", lambda: (df.write.format("iceberg").mode("append").saveAsTable(f"{S}.t_app"), n(f"{S}.t_app"))[1])
p("saveAsTable append (existing)", lambda: (df.write.format("iceberg").mode("append").saveAsTable(f"{S}.t_app"), n(f"{S}.t_app"))[1])
p("saveAsTable overwrite (existing)", lambda: (df.write.format("iceberg").mode("overwrite").saveAsTable(f"{S}.t_app"), n(f"{S}.t_app"))[1])
p("saveAsTable errorifexists (new)", lambda: (df.write.format("iceberg").saveAsTable(f"{S}.t_err"), n(f"{S}.t_err"))[1])
p("CTAS USING iceberg", lambda: (spark.sql(f"CREATE TABLE {S}.t_ctas USING iceberg AS SELECT 1 AS id"), n(f"{S}.t_ctas"))[1])
p("CREATE OR REPLACE TABLE ... AS", lambda: spark.sql(f"CREATE OR REPLACE TABLE {S}.t_corctas USING iceberg AS SELECT 1 AS id").collect())
p("writeTo.using(iceberg).create", lambda: (df.writeTo(f"{S}.t_v2c").using("iceberg").create(), n(f"{S}.t_v2c"))[1])
p("writeTo.createOrReplace", lambda: df.writeTo(f"{S}.t_v2cor").using("iceberg").createOrReplace())
p("CREATE TABLE DDL", lambda: spark.sql(f"CREATE TABLE {S}.t_ddl (id int, v string) USING iceberg").collect())
p("INSERT INTO", lambda: (spark.sql(f"INSERT INTO {S}.t_ddl VALUES (1,'a'),(2,'b')").collect(), n(f"{S}.t_ddl"))[1])
p("INSERT OVERWRITE", lambda: (spark.sql(f"INSERT OVERWRITE {S}.t_ddl SELECT 9, 'z'").collect(), n(f"{S}.t_ddl"))[1])
p("insertInto overwrite", lambda: (df.write.insertInto(f"{S}.t_ddl", overwrite=True), n(f"{S}.t_ddl"))[1])
p("writeTo.overwrite(cond)", lambda: (df.writeTo(f"{S}.t_ddl").overwrite(df.id > 0), n(f"{S}.t_ddl"))[1])
p("DELETE", lambda: (spark.sql(f"DELETE FROM {S}.t_ddl WHERE id = 1").collect(), n(f"{S}.t_ddl"))[1])
p("TRUNCATE", lambda: (spark.sql(f"TRUNCATE TABLE {S}.t_ddl").collect(), n(f"{S}.t_ddl"))[1])
