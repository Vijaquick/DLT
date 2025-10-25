# Delta Live Tables (DLT) Tutorial 

Welcome to the **Delta Live Tables (DLT)** hands-on tutorial using **Databricks**!  
This guide walks you through concepts, code samples, and best practices for building robust ETL pipelines using **DLT, Auto Loader, and SCD logic**, with integrated **data quality checks**.

---

## 🧠 What is Delta Live Tables (DLT)?

**Delta Live Tables (DLT)** is a **declarative ETL framework** built on top of **Delta Lake** in Databricks.  
It simplifies data pipeline creation by managing:
- Table dependencies automatically  
- Data quality enforcement  
- Incremental (streaming) data ingestion  
- Auto-scaling and recovery  

DLT lets you define data transformations as **Python functions** decorated with `@dlt.table` or `@dlt.view`.

# 📚 Delta Live Tables (DLT) — Official Documentation Reference Links

This document provides quick access to all key resources related to **Delta Live Tables (DLT)**, **Auto Loader**, and **Data Quality Expectations** for Databricks.

---

## 🧠 Delta Live Tables (DLT)

### 🔹 DLT Python Developer Guide
Learn how to create and manage Delta Live Tables using Python.

👉 [https://docs.azure.cn/en-us/databricks/dlt/python-dev](https://docs.azure.cn/en-us/databricks/dlt/python-dev)

---

### 🔹 Delta Live Tables Datasets
Understand the core concepts of streaming tables, materialized views, and data pipelines using DLT.

👉 [https://docs.azure.cn/en-us/databricks/delta-live-tables/](https://docs.azure.cn/en-us/databricks/delta-live-tables/)

---

### 🔹 @dlt.table Parameters Reference
Explore all available parameters and configurations for `@dlt.table`, `@dlt.view`, and related decorators.

👉 [https://docs.azure.cn/en-us/databricks/dlt-ref/dlt-python-ref-table](https://docs.azure.cn/en-us/databricks/dlt-ref/dlt-python-ref-table)

---

## ☁️ Auto Loader (Streaming Ingestion)

### 🔹 Auto Loader Overview
Official documentation for Auto Loader — an efficient way to ingest continuously arriving data from cloud storage.

👉 [https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)

---

### 🔹 Common Auto Loader Options
Detailed explanation of configuration options like schema evolution, directory listing mode, and more.

👉 [https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#common-auto-loader-options](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#common-auto-loader-options)

---

## ✅ Data Quality and Expectations in DLT

Learn how to define, enforce, and monitor data quality rules (expectations) in your DLT pipelines.

👉 [https://docs.databricks.com/aws/en/ldp/expectations#gsc.tab=0](https://docs.databricks.com/aws/en/ldp/expectations#gsc.tab=0)

---

## ✨ Summary

| Category | Description | Link |
|-----------|--------------|------|
| DLT Developer Guide | Build Python-based DLT pipelines | [Open Link](https://docs.azure.cn/en-us/databricks/dlt/python-dev) |
| DLT Datasets Overview | Learn about DLT concepts and datasets | [Open Link](https://docs.azure.cn/en-us/databricks/delta-live-tables/) |
| @dlt.table Parameters | Parameters reference for DLT decorators | [Open Link](https://docs.azure.cn/en-us/databricks/dlt-ref/dlt-python-ref-table) |
| Auto Loader Overview | Continuous ingestion with Auto Loader | [Open Link](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/) |
| Auto Loader Options | Configuration and tuning parameters | [Open Link](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#common-auto-loader-options) |
| DLT Expectations | Data quality validation and checks | [Open Link](https://docs.databricks.com/aws/en/ldp/expectations#gsc.tab=0) |

---

### 🧩 Created by: **Vignesan – Creator of Vijaquick**
🎥 [YouTube: Vijaquick](https://www.youtube.com/@vijaquick)  
💡 Simplifying Data Engineering, Analytics, and AI in Tamil.

---

## 🏗️ DLT Concepts

| Concept | Description | Example |
|----------|--------------|----------|
| **@dlt.table** | Creates a **materialized managed table** that stores the output permanently. | `@dlt.table(name="silver_sales")` |
| **@dlt.view** | Creates a **temporary view** used for intermediate logic, not persisted. | `@dlt.view(name="temp_sales")` |
| **Streaming Table** | Reads continuously from streaming sources (like Autoloader). | `spark.readStream.format("cloudFiles")...` |
| **Materialized View** | Similar to table but refreshed periodically, ideal for aggregations. | `@dlt.table(name="gold_sales", materialized=True)` |

---


### Example: Read data from a landing zone
```
#import dlt module
import dlt
from pyspark.sql.functions import*

#Data Quality Rule
_cash_type={
    "validate_cash_type":"cash_type in ('cash','card','UPI')"
}
_price_range={
    "validate_price_range":"money >0",
    "validate_price_range_nul_check":"money is not null"
}

#create bronze table
@dlt.table(
    name="bronze_sales1"
)
@dlt.expect_all_or_fail(_cash_type) #warn
@dlt.expect_all_or_drop(_price_range)
def bronze_coffee_sales():
    coffee_data=spark.readStream.table("lakehouse.sales.coffee_sales")
    return coffee_data
	
@dlt.table(
    name="silver_sales1"
)
def silver_coffee_sales():
    df=spark.read.table("LIVE.bronze_sales1")
    clean_data=df.withColumn("Date",date_format(col("Date"),"dd-MM-yyyy"))\
                .withColumn("_update_date",current_timestamp())
    return clean_data

#Gold Layer

@dlt.table(
    name="gold_sales"
)
def gold_coffee_sales():
    df=spark.read.table("LIVE.silver_sales1")
    agg_data=df.groupBy("Monthsort").agg(sum("money").alias("Total_sales"))
    return agg_data
