# Databricks notebook source
# DBTITLE 1,Medallion 1: Orders - Bronze Layer
# MAGIC %md
# MAGIC # Medallion 1: Orders - Bronze Layer
# MAGIC
# MAGIC This notebook reads raw orders data from the bronze source and creates a cleansed bronze table.
# MAGIC
# MAGIC **Pipeline Flow:**
# MAGIC * Bronze: Read from `data.orders_bronze.orders` and validate ✓
# MAGIC * Silver: Type conversion error ❌
# MAGIC * Gold: Aggregations (would work)

# COMMAND ----------

# DBTITLE 1,Bronze Orders Table
from pyspark.sql import functions as F
from datetime import datetime

# Read raw orders from bronze source
df_raw_orders = spark.table("workspace.orders_bronze.orders")

# Basic validation and standardization
df_bronze = (
    df_raw_orders
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_system", F.lit("orders_api"))
)

print(f"Bronze Orders - Record count: {df_bronze.count()}")
# display(df_bronze.limit(10))

# Write to bronze layer table
df_bronze.write.mode("overwrite").saveAsTable("workspace.orders_silver.bronze_orders_m1")

print("Bronze layer completed successfully")