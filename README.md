# Olist Lakehouse Pipeline

> **Work in progress** — building this step by step, see the checklist below.

End-to-end batch data pipeline on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100k orders, 9 CSVs), built with PySpark on Databricks Free Edition using a bronze / silver / gold (medallion) layout with Delta Lake.

## Goal

Take 9 raw CSVs and turn them into analytics-ready tables that answer questions like:
customer spend & retention, seller performance, delivery time vs. review score, monthly sales trends.

## Stack

- PySpark 3.x on Databricks (serverless)
- Delta Lake + Unity Catalog volume for raw storage
- Databricks SQL dashboard for the serving layer

## Progress

- [x] Setup — schema, volume, upload raw CSVs
- [x] Ingestion — load 9 datasets with schema inference, row/column counts
- [x] Exploration — null report, duplicate check per key, EDA on state / status / payment type, delivery-time distribution
- [ ] Cleaning — drop rows missing keys, flag + fill missing delivery dates *(in progress)*
- [ ] Cleaning — type casting, outlier removal, feature engineering
- [ ] Integration — joins (broadcast for small dims, geolocation pre-aggregated to avoid row explosion)
- [ ] Aggregation — customer / seller / product / monthly metrics, window functions for ranking
- [ ] Optimization — join hints, salting for skew, OPTIMIZE / ZORDER
- [ ] Serving — gold tables + dashboard
- [ ] Docs — data dictionary, ERD

## Findings so far

- `geolocation` has ~1M rows but only ~19k unique zip prefixes → must aggregate before joining or the fact table explodes
- Delivery time: median 10 days, mean 12.5, but max 210 days — long tail worth flagging to logistics
- `order_delivered_customer_date` is null for undelivered orders; filling with a string via `fillna` silently does nothing on a timestamp column — use `coalesce` + `cast` and keep a boolean flag

## Repo layout (target)

```
notebooks/     Databricks notebooks, exported as .py
config/        Spark tuning profile with reasoning
docs/          data dictionary, ERD, dashboard screenshot
```

## Running it

1. Download the dataset from Kaggle and upload the CSVs to `/Volumes/workspace/olist/raw`
2. Import the notebooks in `notebooks/` into your Databricks workspace
3. Run in order
