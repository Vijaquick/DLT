# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view(
    name="customer_data"
)
def customer_data():
  return spark.readStream.table('lakehouse.sales.customer_data')

# COMMAND ----------

dlt.create_streaming_table("scd11_customer_data")

dlt.apply_changes(
    target="scd11_customer_data",
    source="customer_data",
    keys=["customer_id"],
    stored_as_scd_type=1,
    apply_as_deletes=expr("_src_record_status='D'"),
    apply_as_truncates=expr("_src_record_status='T'"),
    sequence_by="_src_ingest_ts"
)

# COMMAND ----------

dlt.create_streaming_table("scd22_customer_data")

dlt.apply_changes(
    target="scd22_customer_data",
    source="customer_data",
    keys=["customer_id"],
    stored_as_scd_type=2,
    apply_as_deletes=expr("_src_record_status='D'"),
    except_column_list=["_src_record_status", "_src_ingest_ts"],
    sequence_by="_src_ingest_ts"
)

# COMMAND ----------

# @dlt.table(
#     name="latest_customer_record"
# )
# def latest_record():
#   return spark.read.table("LIVE.scd2_customer_data").where("__END_AT is null")