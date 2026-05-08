# Databricks notebook source
# DBTITLE 1,Medallion 2: Products - Silver Layer (Works)
# MAGIC %md
# MAGIC # Medallion 2: Products - Silver Layer (Works)
# MAGIC
# MAGIC This notebook transforms bronze products data successfully with data quality improvements.
# MAGIC
# MAGIC **Pipeline Status:**
# MAGIC * Bronze: ✓ Works
# MAGIC * Silver: ✓ **Works fine** - clean transformations
# MAGIC * Gold: ❌ Fails with division by zero

# COMMAND ----------

# DBTITLE 1,Silver Products Transformation
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

print("Starting Silver layer transformation...")

# Read from bronze layer
df_bronze = spark.table("workspace.orders_silver.bronze_products_m2")

print(f"Bronze records loaded: {df_bronze.count()}")

# Clean and enrich product data
df_silver = (
    df_bronze
    .select(
        F.col("PRODUCT_ID").alias("product_id"),
        F.trim(F.col("PRODUCT_NAME")).alias("product_name"),
        # Handle nulls in price and cost - coalesce to 0
        F.coalesce(F.col("PRICE_USD"), F.lit(0.0)).cast(DecimalType(10, 2)).alias("price_usd"),
        F.coalesce(F.col("COGS_USD"), F.lit(0.0)).cast(DecimalType(10, 2)).alias("cogs_usd"),
    )
    .withColumn("processed_at", F.current_timestamp())
    .withColumn("data_quality_flag", 
        F.when((F.col("price_usd") == 0) | (F.col("cogs_usd") == 0), "INCOMPLETE")
        .otherwise("COMPLETE")
    )
)

print("Silver data preview:")
display(df_silver.limit(10))

# Show data quality summary
# print("\nData Quality Summary:")
# df_silver.groupBy("data_quality_flag").count().show()

# Write to silver table
df_silver.write.mode("overwrite").saveAsTable("workspace.orders_silver.silver_products_m2")

print(f"Silver layer completed - {df_silver.count()} products cleaned")