# Databricks notebook source
# DBTITLE 1,Transform Bronze to Silver
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

print("Starting Silver layer transformation...")

# Read from bronze layer
df_bronze = spark.table("workspace.orders_bronze.order_items")

print(f"Bronze records loaded: {df_bronze.count()}")

# Transform to silver with enrichments
df_silver = (
    df_bronze
    # Cast decimal columns to appropriate types
    .withColumn("order_item_id", F.col("ORDER_ITEM_ID").cast(IntegerType()))
    .withColumn("order_id", F.col("ORDER_ID").cast(IntegerType()))
    .withColumn("product_id", F.col("PRODUCT_ID").cast(IntegerType()))
    .withColumn("is_primary_item", F.col("IS_PRIMARY_ITEM").cast(IntegerType()))
    .withColumn("price_usd", F.col("PRICE_USD").cast(DoubleType()))
    .withColumn("cogs_usd", F.col("COGS_USD").cast(DoubleType()))
    .withColumn("created_at", F.col("CREATED_AT"))
    
    # Calculate derived metrics
    .withColumn("profit_usd", F.col("price_usd") - F.col("cogs_usd"))
    .withColumn("profit_margin_pct", 
                F.round((F.col("profit_usd") / F.col("price_usd")) * 100, 2))
    
    # Add data quality indicator
    .withColumn("is_valid", 
                F.when((F.col("price_usd") > 0) & (F.col("cogs_usd") >= 0), True)
                .otherwise(False))
    
    # Extract date parts for partitioning/filtering
    .withColumn("order_date", F.to_date("created_at"))
    .withColumn("order_year", F.year("created_at"))
    .withColumn("order_month", F.month("created_at"))
    
    # Add processing metadata
    .withColumn("processed_at", F.current_timestamp())
    
    # Select final columns in clean order
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "is_primary_item",
        "created_at",
        "order_date",
        "order_year",
        "order_month",
        "price_usd",
        "cogs_usd",
        "profit_usd",
        "profit_margin_pct",
        "is_valid",
        "processed_at"
    )
)

print("Silver transformation completed. Previewing data...")
display(df_silver.limit(10))

# Write to silver table
df_silver.write.mode("overwrite").saveAsTable("workspace.orders_silver.order_items")

print(f"✓ Silver layer completed: {df_silver.count()} records written to workspace.orders_silver.order_items")