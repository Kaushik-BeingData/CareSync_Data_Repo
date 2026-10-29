# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Build gold patient_360 table
spark.sql("CREATE SCHEMA IF NOT EXISTS adb_caresync.gold")

spark.sql("""
 CREATE OR REPLACE TABLE adb_caresync.gold.patient_360 AS
 WITH lab_results_agg AS (
   SELECT
     patient_id,
     CAST(COUNT(*) AS INT) AS total_tests,
     CAST(SUM(CASE WHEN result_status = 'Abnormal' THEN 1 ELSE 0 END) AS INT) AS abnormal_test_count,
     CAST(SUM(CASE WHEN result_status = 'Critical' THEN 1 ELSE 0 END) AS INT) AS critical_test_count,
     MAX(test_date) AS last_test_date
   FROM adb_caresync.silver.lab_results
   GROUP BY patient_id
 )
 SELECT
   p.patient_id,
   p.first_name,
   p.last_name,
   CAST(p.date_of_birth AS DATE) AS date_of_birth,
   CAST(FLOOR(datediff(current_date(), CAST(p.date_of_birth AS DATE)) / 365.25) AS INT) AS age,
   p.gender,
   p.city,
   p.state,
   p.registered_hospital_id AS hospital_id,
   h.hospital_name,
   p.insurance_provider_id,
   ip.provider_name AS insurance_provider_name,
   COALESCE(lra.total_tests, 0) AS total_tests,
   COALESCE(lra.abnormal_test_count, 0) AS abnormal_test_count,
   COALESCE(lra.critical_test_count, 0) AS critical_test_count,
   lra.last_test_date
 FROM adb_caresync.silver.patients AS p
 LEFT JOIN adb_caresync.silver.hospitals AS h
   ON p.registered_hospital_id = h.hospital_id
 LEFT JOIN adb_caresync.silver.insurance_providers AS ip
   ON p.insurance_provider_id = ip.insurance_provider_id
 LEFT JOIN lab_results_agg AS lra
   ON p.patient_id = lra.patient_id
""")

updated_df = spark.table("adb_caresync.gold.patient_360")
record_count = updated_df.count()
dbutils.notebook.exit(str(record_count))

# COMMAND ----------

updated_df.display()

# COMMAND ----------

