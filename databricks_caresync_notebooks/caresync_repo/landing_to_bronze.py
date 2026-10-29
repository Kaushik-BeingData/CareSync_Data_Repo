# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Create and read widgets
# Create widgets
dbutils.widgets.text("source_system", "", "")
dbutils.widgets.text("table_name", "", "")
dbutils.widgets.text("landing_timestamp", "", "")
dbutils.widgets.text("source_file_type","","")

# Get widget values into variables
source_system = dbutils.widgets.get("source_system")
table_name = dbutils.widgets.get("table_name")
landing_timestamp = dbutils.widgets.get("landing_timestamp")
source_file_type =dbutils.widgets.get("source_file_type")

# COMMAND ----------

base_landing_path = "/Volumes/adb_caresync/landing/landing_volume"
source_path = f"{base_landing_path}/{source_system}"
table_path = f"{source_path}/{table_name}"
landing_path = f"{table_path}/{landing_timestamp}/"


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


missing_path = next((path for path in [source_path, table_path, landing_path] if not path_exists(path)), None)

if missing_path:
    message = (
        f"No new files are received for source_system={source_system}, "
        f"table_name={table_name}, landing_timestamp={landing_timestamp}."
    )
    print(message)
    dbutils.notebook.exit(message)

# COMMAND ----------

if source_file_type == "csv":
    df_landing= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(landing_path)
else:
    df_landing= spark.read.format("parquet").load(landing_path)

# COMMAND ----------

df_landing =df_landing.withColumn('File_landing_timestamp',lit(landing_timestamp))\
            .withColumn("t_create_timestamp",current_timestamp())

# COMMAND ----------

df_landing.write.mode("append").saveAsTable(f'adb_caresync.bronze.{table_name}')

# COMMAND ----------

