# Databricks notebook source
# MAGIC %md 
# MAGIC ### Delta Live Tables - DLT
# MAGIC - DLT Document Link https://docs.azure.cn/en-us/databricks/dlt/python-dev
# MAGIC
# MAGIC - Delta Live Tables Datasets https://docs.azure.cn/en-us/databricks/delta-live-tables/
# MAGIC
# MAGIC - @dlt.table Parameters Link https://docs.azure.cn/en-us/databricks/dlt-ref/dlt-python-ref-table
# MAGIC
# MAGIC - Autoloader https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/
# MAGIC
# MAGIC - Auto Loader options https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#common-auto-loader-options
# MAGIC

# COMMAND ----------

#import dlt module
import dlt
from pyspark.sql.functions import*

# COMMAND ----------

#create bronze table
@dlt.table(
    name="bronze_sales"
)
def bronze_coffee_sales():
    coffee_data=spark.readStream.table("lakehouse.sales.coffee_sales")
    return coffee_data

# COMMAND ----------

@dlt.table(
    name="silver_sales"
)
def silver_coffee_sales():
    df=spark.read.table("LIVE.bronze_sales")
    clean_data=df.withColumn("Date",date_format(col("Date"),"dd-MM-yyyy"))\
                .withColumn("_update_date",current_timestamp())
    return clean_data

# COMMAND ----------

# @dlt.view(
#     name="temview_coffee_sales_data"
# )
# def temview_coffee_sales():
#     df=spark.read.table("LIVE.silver_coffee_sales_data")
#     return df

# COMMAND ----------

#Gold Layer

@dlt.table(
    name="gold_sales"
)
def gold_coffee_sales():
    df=spark.read.table("LIVE.silver_sales")
    agg_data=df.groupBy("Monthsort").agg(sum("money").alias("Total_sales"))
    return agg_data