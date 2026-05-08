# Databricks notebook source
# DBTITLE 1,Medallion 2: Products - Bronze Layer
# MAGIC %md
# MAGIC # Medallion 2: Products - Bronze Layer
# MAGIC
# MAGIC This notebook reads raw products data from the bronze source.
# MAGIC
# MAGIC **Pipeline Flow:**
# MAGIC * Bronze: Read from `data.orders_bronze.products` ✓
# MAGIC * Silver: Clean and transform ✓
# MAGIC * Gold: Calculate metrics ❌ **FAILS with division by zero**

# COMMAND ----------

from pyspark.sql import functions as F

df_products = spark.table("workspace.orders_bronze.products").filter(F.col("product_id").isin([1, 2, 3, 4]))
df_orders = spark.table("workspace.orders_bronze.orders")

df_target = (
    df_products
    .join(
        df_orders.select("primary_product_id", "price_usd", "cogs_usd"),
        df_products["product_id"] == df_orders["primary_product_id"],
        "inner"
    )
    .select(
        df_products["product_id"],
        df_products["created_at"],
        df_products["product_name"],
        df_orders["price_usd"],
        df_orders["cogs_usd"]
    )
    .dropDuplicates(["product_id"])
)

display(df_target)

df_target.write.mode("overwrite").saveAsTable("workspace.orders_silver.bronze_products_m2")