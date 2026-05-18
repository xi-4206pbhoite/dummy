# Databricks notebook source
# DBTITLE 1,Medallion 1: Orders - Silver Layer
# MAGIC %md
# MAGIC # Medallion 1: Orders - Silver Layer
# MAGIC
# MAGIC This notebook transforms bronze orders into the silver layer.
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC * Bronze: ✓ Works
# MAGIC * Silver: ✓ Works
# MAGIC * Gold: ✓ Works

# COMMAND ----------

# DBTITLE 1,Silver Orders Transformation
from pyspark.sql import functions as F

print("Starting Silver layer transformation...")

# Read from bronze layer
df_bronze = spark.table("workspace.orders_silver.bronze_orders_m1")

print(f"Bronze records loaded: {df_bronze.count()}")

# Apply transformations
df_silver = (
    df_bronze
    .withColumn("order_id_clean", F.col("ORDER_ID"))
    .withColumn("user_id_clean", F.col("USER_ID"))
    .withColumn("price_usd_clean", F.col("PRICE_USD"))
    .withColumn("processing_timestamp", F.current_timestamp())
)

print("Materializing data...")

# Write to silver table
df_silver.write.mode("overwrite").saveAsTable("workspace.orders_silver.silver_orders_m1")

print("Silver layer completed")