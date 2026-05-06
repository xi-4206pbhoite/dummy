# Databricks notebook source
# DBTITLE 1,Medallion 3: Order Items - Silver Layer (FAILS)
# MAGIC %md
# MAGIC # Medallion 3: Order Items - Silver Layer (FAILS)
# MAGIC
# MAGIC This notebook attempts to transform order items but **fails with ambiguous column reference error**.
# MAGIC
# MAGIC **Error Type:** Ambiguous column reference - attempting to join with a table that has duplicate column names
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC * Bronze: ✓ Works
# MAGIC * Silver: ❌ **FAILS HERE** - Column ambiguity error
# MAGIC * Gold: Would work if silver succeeded

# COMMAND ----------

# DBTITLE 1,Silver Order Items with Column Ambiguity
from pyspark.sql import functions as F

print("Starting Silver layer transformation...")

# Read from bronze layer
df_bronze = spark.table("data.orders_silver.bronze_order_items_m3")

print(f"Bronze records loaded: {df_bronze.count()}")

# Bronze schema sanity check. Run 847380295722602 failed with a cryptic
# AnalysisException at display() time; this assertion fails earlier with the
# missing column named explicitly. `QUANTITY` is currently NOT in bronze and
# requires resolution by the data team before this notebook can run.
expected_cols = {"ORDER_ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "PRICE_USD"}
missing = expected_cols - set(df_bronze.columns)
assert not missing, (
    f"bronze missing required columns: {sorted(missing)}. "
    "Confirm column lineage with data-owners before re-running."
)

df_with_product = df_bronze.select(
    F.col("ORDER_ITEM_ID"),
    F.col("ORDER_ID"),
    F.col("PRODUCT_ID"),
    F.col("QUANTITY"),
    F.col("PRICE_USD"),
)

df_silver = (
    df_with_product
    .select(
        "ORDER_ITEM_ID",
        "ORDER_ID",
        "PRODUCT_ID",
        "QUANTITY",
        (F.col("QUANTITY") * F.col("PRICE_USD")).alias("line_total_usd"),
    )
    .withColumn("processed_at", F.current_timestamp())
)

print("Attempting to materialize data...")
display(df_silver.limit(10))

df_silver.write.mode("overwrite").saveAsTable("data.orders_silver.silver_order_items_m3")

print("✓ Silver layer completed")