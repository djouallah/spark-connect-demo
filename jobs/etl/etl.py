"""ETL: raw files -> cleaned, enriched tables.

    --raw <dir with orders/ customers/ products/ fx/>  --schema <catalog.schema>  [--format iceberg]

Progress goes through `log`, not print (some platforms only keep `logging` output).
"""
import logging, re, sys
import pandas as pd
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import pandas_udf

log = logging.getLogger("quack.etl")
log.setLevel(logging.INFO)
spark = SparkSession.builder.getOrCreate()


def arg(name, default=None):
    """Job argument `--name value`. Every platform-specific value (paths, schema, format) comes in this way."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    if default is None:
        raise SystemExit(f"missing job argument {name}")
    return default


# Which engine runs this script. Every `if ENGINE == ...` below is a difference between engines (findings/<engine>.md).
try:
    from snowflake.snowpark.context import get_active_session
    get_active_session()                         # only Snowflake Code Bundles have a Snowpark session
    ENGINE = "snowflake"
except Exception:
    # Spark SQL version() is the engine's own version: spark.version starts with it on Spark (Fabric adds a suffix), not on Sail
    ENGINE = "spark" if spark.version.startswith(spark.sql("SELECT version()").first()[0].split()[0]) else "sail"

RAW = arg("--raw")
SCHEMA = arg("--schema")
FMT = arg("--format", "iceberg")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
DIAL = {"AU": "61", "NZ": "64", "US": "1"}


def clean_strings(df):
    """Trim every string column and turn empty strings into NULL -- driven by the schema."""
    return df.select([
        (F.when(F.trim(c) == "", None).otherwise(F.trim(c)) if t == "string" else F.col(c)).alias(c)
        for c, t in df.dtypes
    ])


@F.udf("string")
def to_e164(phone, country):
    """Python UDF: normalise '0412 345 678', '(415) 555-0100', '+61412345678' to E.164."""
    if phone is None or country not in DIAL:
        return None
    digits = re.sub(r"\D", "", phone)
    code = DIAL[country]
    if digits.startswith(code) and phone.strip().startswith("+"):
        return "+" + digits
    return "+" + code + digits.lstrip("0")


@pandas_udf("double")
def to_usd(amount: pd.Series, rate: pd.Series) -> pd.Series:
    """pandas (vectorised) UDF: convert to USD."""
    return amount * rate


# ---------------- extract ----------------
csv = spark.read.option("header", True)
orders_raw = csv.csv(f"{RAW}/orders/")
customers_raw = spark.read.json(f"{RAW}/customers/")
products = csv.option("inferSchema", True).csv(f"{RAW}/products/")
fx = csv.option("inferSchema", True).csv(f"{RAW}/fx/")
log.info("EXTRACT|orders_raw=%d customers_raw=%d products=%d fx=%d",
         orders_raw.count(), customers_raw.count(), products.count(), fx.count())

# ---------------- clean orders ----------------
orders = (clean_strings(orders_raw)
          .withColumn("order_id", F.upper("order_id"))
          .withColumn("customer_id", F.upper("customer_id"))
          .withColumn("sku", F.upper("sku"))
          .withColumn("status", F.lower("status"))
          .withColumn("currency", F.upper("currency"))
          .withColumn("amount", F.regexp_replace("amount", r"[$,]", "").cast("double"))
          .withColumn("order_ts", F.coalesce(F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
                                             F.to_timestamp("order_ts", "dd/MM/yyyy HH:mm:ss")))
          .dropDuplicates())

is_valid = F.col("order_id").isNotNull() & F.col("order_ts").isNotNull()
rejects = orders.filter(~is_valid)
orders = orders.filter(is_valid)
log.info("CLEAN|orders=%d rejects=%d", orders.count(), rejects.count())

# ---------------- customers: latest version, flattened, phone normalised ----------------
latest = Window.partitionBy("customer_id").orderBy(F.to_timestamp("updated_at").desc())
dim_customer = (clean_strings(customers_raw.select("customer_id", "name", "email", "phone",
                                                   F.col("address.city").alias("city"),
                                                   F.col("address.country").alias("country"),
                                                   "updated_at"))
                .withColumn("rn", F.row_number().over(latest))
                .filter("rn = 1").drop("rn")
                .withColumn("phone_e164", to_e164("phone", "country"))
                .withColumn("updated_at", F.to_timestamp("updated_at")))

# ---------------- enrich ----------------
fct_orders = (orders
              .join(dim_customer.select("customer_id", "country"), "customer_id")
              .join(products.select("sku", "category"), "sku")
              .join(F.broadcast(fx), "currency")
              .withColumn("amount_usd", to_usd("amount", "rate_to_usd"))
              .withColumn("order_size", F.when(F.col("amount_usd") < 50, "small")
                                         .when(F.col("amount_usd") < 500, "medium")
                                         .otherwise("large"))
              .withColumn("order_date", F.to_date("order_ts"))
              .select("order_id", "order_ts", "order_date", "customer_id", "country", "sku", "category",
                      "status", "currency", "amount", "rate_to_usd", "amount_usd", "order_size"))

agg_daily_country = (fct_orders.groupBy("order_date", "country")
                     .agg(F.count("*").alias("orders"),
                          F.round(F.sum("amount_usd"), 2).alias("revenue_usd"),
                          F.countDistinct("customer_id").alias("customers"))
                     .orderBy("order_date", "country"))

# ---------------- load ----------------
# V1 API
if ENGINE == "sail":        # NON-STANDARD: Sail can't replace a table on the OneLake Iceberg catalog, so drop it and create it
    spark.sql("DROP TABLE IF EXISTS ETL_DIM_CUSTOMER")
    dim_customer.write.format(FMT).saveAsTable("ETL_DIM_CUSTOMER")
else:
    dim_customer.write.format(FMT).mode("overwrite").saveAsTable("ETL_DIM_CUSTOMER")
v2 = fct_orders.writeTo("ETL_FCT_ORDERS").using(FMT).partitionedBy("order_date")  # V2 API; a plain column, which every table format supports
if ENGINE == "sail":        # NON-STANDARD: Sail can't replace a table on the OneLake Iceberg catalog, so drop it and create it
    spark.sql("DROP TABLE IF EXISTS ETL_FCT_ORDERS")
    v2.create()
else:
    v2.createOrReplace()
if ENGINE == "sail":        # NON-STANDARD: Sail can't replace a table on the OneLake Iceberg catalog, so drop it and create it
    spark.sql("DROP TABLE IF EXISTS ETL_AGG_DAILY_COUNTRY")
    agg_daily_country.write.format(FMT).saveAsTable("ETL_AGG_DAILY_COUNTRY")
else:
    agg_daily_country.write.format(FMT).mode("overwrite").saveAsTable("ETL_AGG_DAILY_COUNTRY")

log.info("LOAD|dim_customer=%d fct_orders=%d agg_daily_country=%d",
         spark.table("ETL_DIM_CUSTOMER").count(),
         spark.table("ETL_FCT_ORDERS").count(),
         spark.table("ETL_AGG_DAILY_COUNTRY").count())
