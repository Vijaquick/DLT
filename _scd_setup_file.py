# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists lakehouse.sales.customer_data(
# MAGIC   customer_id int,
# MAGIC   customer_name string,
# MAGIC   customer_address string,
# MAGIC   email string,
# MAGIC   _src_record_status string, --I,U,D
# MAGIC   _src_ingest_ts timestamp
# MAGIC ) using delta
# MAGIC comment "scd type demo table"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table lakehouse.sales.customer_data
# MAGIC values
# MAGIC (1001,'John Joseph','123 Main St','john.doe@vijaquick.com','I',current_timestamp - interval '5 days'),
# MAGIC (1002,'Siva Saravanan','456 Oak Ave','siva.saravanan@vijaquick.com','I',current_timestamp - interval '5 days'),
# MAGIC (1003,'Raj Sundharam','789 Pine Rd','raj.sundharam@vijaquick.com','I',current_timestamp - interval '3 day')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse.sales.customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table lakehouse.sales.customer_data
# MAGIC values
# MAGIC (1003,'Raj Sundharam','789 Pine Rd','raj.sundharam1@vijaquick.com','I',current_timestamp - interval '4 day')

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table lakehouse.sales.customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse.dlt.scd11_customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lakehouse.dlt.scd22_customer_data