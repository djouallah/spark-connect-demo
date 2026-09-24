from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Messy input: mixed-case column names, padded strings, a duplicate row
raw = spark.createDataFrame(
    [
        (1, "  AU ", " Widget", 3, 10.0, False),
        (1, "  AU ", " Widget", 3, 10.0, False),   # duplicate
        (2, "NZ",    "Gadget ", 1, 25.0, False),
        (3, " AU",   "Gadget",  2, 25.0, True),
        (4, "US ",   "Widget",  5, 9.5,  False),
        (5, "AU",    " Gizmo ", 4, 7.0,  False),
    ],
    ["Order_ID", "Country", "Product", "Qty", "Price", "Is_Return"],
)

# 1. Bulk transform driven by the schema: trim every string col, lowercase every name
clean = raw.select([
    (F.trim(F.col(c)) if t == "string" else F.col(c)).alias(c.lower())
    for c, t in raw.dtypes
])

# 2. Conditional pipeline building
def orders(df, country=None, include_returns=False):
    if country:
        df = df.filter(F.col("country") == country)
    if not include_returns:
        df = df.filter(~F.col("is_return"))
    return df

# 3. Reusable, composable steps
def dedupe(keys):
    return lambda df: df.dropDuplicates(keys)

def add_revenue(df):
    return df.withColumn("revenue", F.col("qty") * F.col("price"))

def ensure_col(name, default):
    # 5. schema introspection mid-pipeline (AnalyzePlan round-trip under Spark Connect)
    return lambda df: df if name in df.columns else df.withColumn(name, F.lit(default))

pipeline = (orders(clean)
            .transform(dedupe(["order_id"]))
            .transform(add_revenue)
            .transform(ensure_col("discount", 0.0)))

# 4. Dynamic aggregation from config
keys = ["country", "product"]
metrics = {"qty": "sum", "revenue": "sum", "order_id": "count"}
result = (pipeline.groupBy(*keys)
          .agg(*[F.expr(f"{fn}({c})").alias(f"{c}_{fn}") for c, fn in metrics.items()])
          .orderBy(*keys))

print("clean columns:", clean.columns)
print("pipeline columns:", pipeline.columns)
result.printSchema()
result.show()

result.write.mode("overwrite").saveAsTable("CODE_BUNDLE_DEMO")
print("rows written:", spark.table("CODE_BUNDLE_DEMO").count())
