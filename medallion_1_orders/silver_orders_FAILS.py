# Databricks notebook source
# DBTITLE 1,Medallion 1: Orders - Silver Layer (FAILS)
# MAGIC %md
# MAGIC # Medallion 1: Orders - Silver Layer (FAILS)
# MAGIC
# MAGIC This notebook attempts to transform bronze orders but **fails with a data type conversion error**.
# MAGIC
# MAGIC **Error Type:** Invalid cast - attempting to convert CREATED_AT (timestamp string format '2024-01-15 10:30:45') to integer
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC * Bronze: ✓ Works
# MAGIC * Silver: ❌ **FAILS HERE** - Data type mismatch
# MAGIC * Gold: Would work if silver succeeded

# COMMAND ----------

# DBTITLE 1,Silver Orders with Type Error
from pyspark.sql import functions as F


print("Starting Silver layer transformation...")

# Read from bronze layer
df_bronze = spark.table("workspace.orders_silver.bronze_orders_m1")

print(f"Bronze records loaded: {df_bronze.count()}")

# Apply transformations with TYPE ERROR
df_silver = (
    df_bronze
    .withColumn("order_id_clean", F.col("ORDER_ID"))
    .withColumn("user_id_clean", F.col("USER_ID"))
    .withColumn("price_usd_clean", F.col("PRICE_USD"))
    .withColumn("processing_timestamp", F.current_timestamp())
)

print("Attempting to materialize data...")
# This will trigger the error when Spark tries to execute the transformation
# display(df_silver.limit(10))

# Write to silver table (won't reach here due to error)
df_silver.write.mode("overwrite").saveAsTable("workspace.orders_silver.silver_orders_m1")

print("Silver layer completed")