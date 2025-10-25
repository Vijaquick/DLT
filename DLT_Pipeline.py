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

cash_type=spark.conf.get("cash_type")

# COMMAND ----------

@dlt.table(
    name="bronze_sales_streaming1"
)
def bronze_coffee_sales():
    coffee_data = spark.readStream.table("lakehouse.sales.coffee_sales")
    return coffee_data

# COMMAND ----------

@dlt.table(
    name="bronze_sales_streaming2"
)
def bronze_coffee_sales():
    coffee_data = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaHints",
                "hour_of_day INT, cash_type STRING, money DOUBLE, coffee_name STRING, Time_of_Day STRING, Weekday STRING, Month_name STRING, Weekdaysort INT, Monthsort INT, Date DATE, Time TIMESTAMP")
        .option("cloudFiles.schemaLocation", "/Volumes/lakehouse/sales/coffee_sales_file/schema/")
        .option("pathGlobFilter", "*.csv")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .load("/Volumes/lakehouse/sales/coffee_sales_file/landing_page/")
    )
    return coffee_data

# COMMAND ----------

dlt.create_streaming_table("union_coffee_sales_data")

#append stream1 table
@dlt.append_flow(
    target="union_coffee_sales_data"
)
def stream1():
    return spark.readStream.table("LIVE.bronze_sales_streaming1")

#append stream2 table
@dlt.append_flow(
    target="union_coffee_sales_data"
)
def stream2():
    return spark.readStream.table("LIVE.bronze_sales_streaming2")

# COMMAND ----------

# @dlt.table(
#     name='union_coffee_sales'
# )

# def union_coffee_sales():
#     df1=spark.read.table("LIVE.bronze_sales_streaming1")
#     df2=spark.read.table("LIVE.bronze_sales_streaming2")
#     union_df=df1.union(df2)
#     return union_df

# COMMAND ----------

@dlt.table(
    name="silver_sales_day"
)
def silver_coffee_sales():
    df=spark.read.table("LIVE.union_coffee_sales_data")
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
    df=spark.read.table("LIVE.silver_sales_day")
    agg_data=df.groupBy("Monthsort").agg(sum("money").alias("Total_sales"))
    return agg_data

# COMMAND ----------

for param in cash_type.split(","):
    
    @dlt.table(
        name=f"gold_sales_{param}"
    )
    def gold_coffee_sales():
        df=spark.read.table("LIVE.silver_sales_day")
        agg_data = df.where(f"cash_type== '{param}' ").groupBy("Monthsort").agg(sum("money").alias("Total_sales"))
        return agg_data
 