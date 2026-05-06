# Databricks notebook source
# DBTITLE 1,Medallion 2: Products - Gold Layer (FAILS)
# MAGIC %md
# MAGIC # Medallion 2: Products - Gold Layer (FAILS)
# MAGIC
# MAGIC This notebook attempts to calculate product profitability metrics but **fails with division by zero error**.
# MAGIC
# MAGIC **Error Type:** Division by zero when calculating profit margin percentage (dividing by price_usd which can be 0 from our silver layer)
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC * Bronze: ✓ Works
# MAGIC * Silver: ✓ Works (but includes products with price_usd = 0)
# MAGIC * Gold: ❌ **FAILS HERE** - Arithmetic error during metric calculation

# COMMAND ----------

# DBTITLE 1,Gold Products Metrics with Division Error
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

print("Starting Gold layer metrics calculation...")

# Read from silver layer
df_silver = spark.table("data.orders_silver.silver_products_m2")

print(f"Silver records loaded: {df_silver.count()}")

# Calculate product profitability metrics.
#
# Run 194419570945337 failed with NUMERIC_VALUE_OUT_OF_RANGE because (a) the
# margin formula had a stray `- price_usd` term that made the result roughly
# proportional to price, and (b) the cast was to DecimalType(5, 2) (max
# +/-999.99). Both bugs are fixed below; price_usd = 0 is also guarded with
# nullIf so a zero price yields NULL rather than a division-by-zero under ANSI.
df_gold = (
    df_silver
    .select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("price_usd"),
        F.col("cogs_usd"),
        (F.col("price_usd") - F.col("cogs_usd")).alias("profit_usd"),
        (
            ((F.col("price_usd") - F.col("cogs_usd")) / F.nullif(F.col("price_usd"), F.lit(0)))
            * 100
        )
            .cast(DecimalType(7, 2))
            .alias("profit_margin_pct"),
    )
    .withColumn("metrics_calculated_at", F.current_timestamp())
)

print("Attempting to calculate profit margins...")
# This will trigger the division by zero error
display(df_gold.limit(10))

# Write to gold table (won't reach here due to error)
df_gold.write.mode("overwrite").saveAsTable("data.orders_gold.product_profitability_m2")

print("✓ Gold layer completed")