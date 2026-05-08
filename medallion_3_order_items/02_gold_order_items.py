# Databricks notebook source
# DBTITLE 1,Product Performance Metrics
from pyspark.sql import functions as F

print("Creating Product Performance metrics...")

# Read from silver layer
df_silver = spark.table("workspace.orders_silver.order_items")

# Aggregate by product
df_product_performance = (
    df_silver
    .filter(F.col("is_valid") == True)
    .groupBy("product_id")
    .agg(
        F.count("order_item_id").alias("total_items_sold"),
        F.countDistinct("order_id").alias("unique_orders"),
        F.sum("price_usd").alias("total_revenue_usd"),
        F.sum("cogs_usd").alias("total_cogs_usd"),
        F.sum("profit_usd").alias("total_profit_usd"),
        F.avg("profit_margin_pct").alias("avg_profit_margin_pct"),
        F.min("created_at").alias("first_sale_date"),
        F.max("created_at").alias("last_sale_date")
    )
    .withColumn("roi_pct", 
                F.round((F.col("total_profit_usd") / F.col("total_cogs_usd")) * 100, 2))
    .orderBy(F.desc("total_revenue_usd"))
)

print("Previewing product performance...")
display(df_product_performance.limit(10))

df_product_performance.write.mode("overwrite").saveAsTable("workspace.orders_gold.product_performance")

print(f"Product performance created: {df_product_performance.count()} products")

# COMMAND ----------

# DBTITLE 1,Daily Order Summary
print("Creating Daily Order Summary...")

df_silver = spark.table("workspace.orders_silver.order_items")

# Aggregate by date
df_daily_summary = (
    df_silver
    .filter(F.col("is_valid") == True)
    .groupBy("order_date", "order_year", "order_month")
    .agg(
        F.count("order_item_id").alias("total_items"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("product_id").alias("unique_products_sold"),
        F.sum("is_primary_item").alias("primary_items_count"),
        F.sum("price_usd").alias("daily_revenue_usd"),
        F.sum("cogs_usd").alias("daily_cogs_usd"),
        F.sum("profit_usd").alias("daily_profit_usd"),
        F.avg("price_usd").alias("avg_item_price_usd"),
        F.avg("profit_margin_pct").alias("avg_profit_margin_pct")
    )
    .withColumn("items_per_order", 
                F.round(F.col("total_items") / F.col("total_orders"), 2))
    .orderBy("order_date")
)

print("Previewing daily summary...")
display(df_daily_summary.limit(10))

df_daily_summary.write.mode("overwrite").saveAsTable("workspace.orders_gold.daily_order_summary")

print(f" Daily summary created: {df_daily_summary.count()} days")

# COMMAND ----------

# DBTITLE 1,Monthly Metrics
print("Creating Monthly Metrics...")

df_silver = spark.table("workspace.orders_silver.order_items")

# Aggregate by year and month
df_monthly_metrics = (
    df_silver
    .filter(F.col("is_valid") == True)
    .groupBy("order_year", "order_month")
    .agg(
        F.count("order_item_id").alias("total_items"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("product_id").alias("unique_products"),
        F.sum("price_usd").alias("monthly_revenue_usd"),
        F.sum("cogs_usd").alias("monthly_cogs_usd"),
        F.sum("profit_usd").alias("monthly_profit_usd"),
        F.avg("profit_margin_pct").alias("avg_profit_margin_pct"),
        F.max("price_usd").alias("max_item_price_usd"),
        F.min("price_usd").alias("min_item_price_usd")
    )
    .withColumn("month_year", F.concat(F.col("order_year"), F.lit("-"), 
                                        F.lpad(F.col("order_month"), 2, "0")))
    .orderBy("order_year", "order_month")
)

print("Previewing monthly metrics...")
display(df_monthly_metrics.limit(10))

df_monthly_metrics.write.mode("overwrite").saveAsTable("workspace.orders_gold.monthly_metrics")

print(f"Monthly metrics created: {df_monthly_metrics.count()} months")