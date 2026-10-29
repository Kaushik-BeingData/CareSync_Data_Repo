# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("Table Name", "", "")
dbutils.widgets.text("Landing Timestamp", "", "")
dbutils.widgets.text("merge_keys", "", "")
dbutils.widgets.text("load_type", "", "")
# Get widget values into variables
table_name = dbutils.widgets.get("Table Name")
landing_timestamp = dbutils.widgets.get("Landing Timestamp")
merge_keys = dbutils.widgets.get("merge_keys")
load_type = dbutils.widgets.get("load_type")

# COMMAND ----------

df_bronze =spark.read.table(f'adb_caresync.bronze.{table_name}')


# COMMAND ----------

df_bronze = (df_bronze
             .withColumn('sl_insert_timestamp',current_timestamp())
             .withColumn('t_update_timestamp',current_timestamp())
             .filter(col('File_landing_timestamp') == f"{landing_timestamp}")
             )

# COMMAND ----------

# DBTITLE 1,Write to Silver based on Load Type
from delta.tables import DeltaTable

target_table = f"adb_caresync.silver.{table_name}"
load_type_norm = (load_type or "FULL").strip().upper()
merge_keys = [k.strip() for k in merge_keys.split(",") if k.strip()]

if load_type_norm == "FULL":
    # Full load: overwrite the target table
    df_bronze.write.mode("overwrite").saveAsTable(target_table)

elif load_type_norm == "APPEND":
    # Append: add incoming records to the target table
    df_bronze.write.mode("append").saveAsTable(target_table)

elif load_type_norm == "MERGE":
    if not merge_keys:
        raise ValueError("Merge Keys widget must be provided for MERGE load type (comma-separated).")

    if not spark.catalog.tableExists(target_table):
        # First run: create the target table
        df_bronze.write.mode("overwrite").saveAsTable(target_table)
    else:
        # Deduplicate source on merge keys to avoid multiple-match errors
        df_source = df_bronze.dropDuplicates(merge_keys)
        merge_condition = " AND ".join([f"tgt.`{k}` <=> src.`{k}`" for k in merge_keys])

        # On update, keep the original insert timestamp
        update_cols = {f"`{c}`": f"src.`{c}`" for c in df_source.columns if c != "sl_insert_timestamp"}

        (DeltaTable.forName(spark, target_table).alias("tgt")
            .merge(df_source.alias("src"), merge_condition)
            .whenMatchedUpdate(set=update_cols)
            .whenNotMatchedInsertAll()
            .execute())

else:
    raise ValueError(f"Unsupported Load Type '{load_type}'. Expected FULL, APPEND or MERGE.")

# COMMAND ----------

record_count = df_bronze.count()
dbutils.notebook.exit(str(record_count))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adb_caresync.silver.insurance_providers;

# COMMAND ----------

