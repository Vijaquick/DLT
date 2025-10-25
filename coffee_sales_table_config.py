# Databricks notebook source
# MAGIC %md 
# MAGIC ### Delta Live Table 
# MAGIC - create catalog and schema and table
# MAGIC - add coffee sales data

# COMMAND ----------

# MAGIC %sql
# MAGIC --create catalog
# MAGIC create catalog if not exists lakehouse;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --create schema
# MAGIC create schema if not exists lakehouse.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC ---create coffe sales table
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse.sales.coffee_sales (
# MAGIC     hour_of_day INT,
# MAGIC     cash_type STRING,
# MAGIC     money DOUBLE,
# MAGIC     coffee_name STRING,
# MAGIC     Time_of_Day STRING,
# MAGIC     Weekday STRING,
# MAGIC     Month_name STRING,
# MAGIC     Weekdaysort INT,
# MAGIC     Monthsort INT,
# MAGIC     Date DATE,
# MAGIC     Time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Coffee sales dataset'

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
coffee_sales = spark.read.option("header","true").option("inferSchema","true").csv("/Volumes/workspace/vijaquick/sales_data/Coffe_sales.csv")

coffee_sales.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakehouse.sales.coffee_sales VALUES
# MAGIC (9,  'card', 22.3, 'Latte',      'Morning',   'Tue', 'Jan', 2,  1, '2024-01-09', '2025-10-11T09:05:45.606+00:00'),
# MAGIC (10, 'card', 18.0, 'Cappuccino', 'Morning',   'Wed', 'Jan', 3,  1, '2024-01-10', '2025-10-11T10:12:33.606+00:00'),
# MAGIC (11, 'cash', 25.7, 'Mocha',      'Afternoon', 'Thu', 'Feb', 4,  2, '2024-02-15', '2025-10-11T11:40:22.606+00:00'),
# MAGIC (12, 'card', 28.9, 'Americano',  'Afternoon', 'Fri', 'Mar', 5,  3, '2024-03-07', '2025-10-11T12:30:12.606+00:00'),
# MAGIC (13, 'cash', 20.4, 'Flat White', 'Afternoon', 'Sat', 'Mar', 6,  3, '2024-03-09', '2025-10-11T13:45:54.606+00:00'),
# MAGIC (14, 'card', 26.8, 'Macchiato',  'Afternoon', 'Sun', 'Apr', 7,  4, '2024-04-14', '2025-10-11T14:05:18.606+00:00'),
# MAGIC (15, 'cash', 23.1, 'Cortado',    'Evening',   'Mon', 'Apr', 1,  4, '2024-04-15', '2025-10-11T15:25:42.606+00:00'),
# MAGIC (16, 'card', 30.0, 'Cold Brew',  'Evening',   'Tue', 'May', 2,  5, '2024-05-07', '2025-10-11T16:10:30.606+00:00'),
# MAGIC (17, 'cash', 19.8, 'Affogato',   'Evening',   'Wed', 'May', 3,  5, '2024-05-08', '2025-10-11T17:50:55.606+00:00'),
# MAGIC (18, 'card', 27.4, 'Espresso',   'Morning',   'Thu', 'Jun', 4,  6, '2024-06-06', '2025-10-11T08:25:37.606+00:00'),
# MAGIC (19, 'cash', 22.7, 'Latte',      'Morning',   'Fri', 'Jun', 5,  6, '2024-06-07', '2025-10-11T09:35:41.606+00:00'),
# MAGIC (20, 'card', 31.2, 'Cappuccino', 'Afternoon', 'Sat', 'Jul', 6,  7, '2024-07-13', '2025-10-11T12:15:22.606+00:00'),
# MAGIC (21, 'cash', 24.9, 'Mocha',      'Afternoon', 'Sun', 'Jul', 7,  7, '2024-07-14', '2025-10-11T13:00:11.606+00:00'),
# MAGIC (22, 'card', 29.5, 'Americano',  'Afternoon', 'Mon', 'Aug', 1,  8, '2024-08-12', '2025-10-11T14:45:05.606+00:00'),
# MAGIC (23, 'cash', 17.9, 'Flat White', 'Evening',   'Tue', 'Aug', 2,  8, '2024-08-13', '2025-10-11T17:05:15.606+00:00'),
# MAGIC (7,  'card', 21.0, 'Macchiato',  'Morning',   'Wed', 'Sep', 3,  9, '2024-09-11', '2025-10-11T07:45:18.606+00:00'),
# MAGIC (9,  'cash', 19.3, 'Cortado',    'Morning',   'Thu', 'Sep', 4,  9, '2024-09-12', '2025-10-11T09:10:30.606+00:00'),
# MAGIC (10, 'card', 27.8, 'Cold Brew',  'Afternoon', 'Fri', 'Oct', 5, 10, '2024-10-11', '2025-10-11T10:40:22.606+00:00'),
# MAGIC (15, 'card', 28.9, 'Americano',  'Afternoon', 'Thu', 'Mar', 4,  3, '2024-03-07', '2025-10-11T15:40:22.606+00:00');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakehouse.sales.coffee_sales VALUES
# MAGIC (758,  null , 0, 'Latte',      'Morning',   'Tue', 'Jan', 2,  1, '2024-01-09', '2025-10-11T09:05:45.606+00:00')

# COMMAND ----------

coffee_sales.write.format("delta").mode("append").saveAsTable("lakehouse.sales.coffee_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists lakehouse.sales.coffee_sales_file
# MAGIC comment 'Daily coffee sales csv file';

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/lakehouse/sales/coffee_sales_file/landing_page/")
dbutils.fs.mkdirs("/Volumes/lakehouse/sales/coffee_sales_file/schema/")