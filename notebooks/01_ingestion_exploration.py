# Databricks notebook source
# MAGIC %md
# MAGIC # Olist Lakehouse Playbook

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.olist;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.olist.raw;

# COMMAND ----------

RAW = "/Volumes/workspace/olist/raw"

for f in dbutils.fs.ls(RAW):
    print(f"{f.name:<48s} {f.size/1_048_576:8.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion & Exploration
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

RAW = "/Volumes/workspace/olist/raw"
CATALOG = "workspace"
SCHEMA = "olist"

FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "categories": "product_category_name_translation.csv",
}

dfs = {}

for name, filename in FILES.items():
    dfs[name] = (
        spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(f"{RAW}/{filename}")
    )

for name, df in dfs.items():
    print(f"{name:<14s} rows={df.count():>10,d} cols={len(df.columns):>2}")


# COMMAND ----------

# MAGIC %md
# MAGIC ตรวจ missing values

# COMMAND ----------

def missing_report(df, name):
    total = df.count()

    exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]
    counts = df.select(*exprs).first().asDict()

    rows = [
        (name, col, n, round(100 * n / total, 2))
        for col, n in counts.items() if n > 0
    ]
    return spark.createDataFrame(
        rows,
        "dataset string, column string, null_count long, null_pct double"
    )

for name, df in dfs.items():
    rep = missing_report(df, name)
    if rep.count() > 0:
        display(rep)


# COMMAND ----------

# MAGIC %md
# MAGIC ตรวจ duplicate

# COMMAND ----------

def duplicate_report(df, key):
    return (df.groupBy(key)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count")))
    
display(duplicate_report(dfs["customers"], "customer_id"))
display(duplicate_report(dfs["orders"],    "order_id"))   
display(duplicate_report(dfs["geolocation"], "geolocation_zip_code_prefix"))            


# COMMAND ----------

# MAGIC %md
# MAGIC EDA

# COMMAND ----------

display(dfs["customers"].groupBy("customer_state").count().orderBy(F.desc("count")))
display(dfs["orders"].groupBy("order_status").count().orderBy(F.desc("count")))
display(dfs["payments"].groupBy("payment_type").count().orderBy(F.desc("count")))
display(dfs["order_items"].groupBy("product_id")
        .agg(F.round(F.sum("price"), 2).alias("total_sales"))
        .orderBy(F.desc("total_sales"))
        .limit(10))
        

# COMMAND ----------

# MAGIC %md
# MAGIC Delivery time analysis

# COMMAND ----------

delivery = (dfs["orders"]
            .withColumn("delivery_days",
                        F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
            )
            .select("order_id", "order_status", "order_purchase_timestamp", "order_delivered_customer_date", "delivery_days"))

display(delivery.orderBy(F.desc("delivery_days")).limit(20))
display(delivery.select("delivery_days").summary())
            
            

# COMMAND ----------

delivery.filter(F.col("delivery_days") > 100).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning & Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ตัดแถวที่ขาด key สำคัญ

# COMMAND ----------

orders_c = dfs["orders"].na.drop(
    subset=["order_id", "customer_id", "order_status"])

# COMMAND ----------

# MAGIC %md
# MAGIC จัดการวันที่จัดส่งที่หายไป

# COMMAND ----------

orders_c = (orders_c
            .withColumn("is_delivery_date_missing",
                        F.col("order_delivered_customer_date").isNull())
            .withColumn("order_delivered_customer_date",
                        F.coalesce(F.col("order_delivered_customer_date"), F.lit("9999-12-31").cast("timestamp")))
)
            

# COMMAND ----------

# MAGIC %md
# MAGIC