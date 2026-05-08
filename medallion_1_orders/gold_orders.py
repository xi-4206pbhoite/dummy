# Databricks notebook source
# DBTITLE 1,Medallion 1: Orders - Gold Layer
# MAGIC %md
# MAGIC # Medallion 1: Orders - Gold Layer
# MAGIC
# MAGIC This notebook aggregates silver orders data into business metrics.
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC * Bronze: ✓ Works
# MAGIC * Silver: ❌ Fails with type conversion error
# MAGIC * Gold: ✓ **This code is correct but won't run** because silver table doesn't exist
# MAGIC
# MAGIC **Note:** This layer would work fine if the silver layer succeeded.

# COMMAND ----------

# DBTITLE 1,Gold Orders Aggregation
from pyspark.sql import functions as F

print("Starting Gold layer aggregation...")

# This will fail because silver table doesn't exist (silver layer failed)
df_silver = spark.table("data.orders_silver.silver_orders_m1")

print(f"Silver records loaded: {df_silver.count()}")

# Aggregate orders by user
df_gold = (
    df_silver
    .groupBy("user_id_clean")
    .agg(
        F.count("order_id_clean").alias("total_orders"),
        F.sum("price_usd_clean").alias("total_spent_usd"),
        F.avg("price_usd_clean").alias("avg_order_value_usd"),
        F.min("processing_timestamp").alias("first_order_processed"),
        F.max("processing_timestamp").alias("last_order_processed")
    )
    .withColumn("report_date", F.current_date())
)

print("Preview of gold aggregations:")
display(df_gold.limit(10))

# Write to gold table
df_gold.write.mode("overwrite").saveAsTable("data.orders_gold.user_order_summary_m1")

print("✓ Gold layer completed successfully")