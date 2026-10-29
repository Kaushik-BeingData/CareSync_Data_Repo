# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Build gold hospital_daily_summary table
spark.sql("CREATE SCHEMA IF NOT EXISTS adb_caresync.gold")

spark.sql("""
CREATE OR REPLACE TABLE adb_caresync.gold.hospital_daily_summary AS
WITH daily_metrics AS (
  SELECT
    hospital_id,
    test_date AS summary_date,
    CAST(COUNT(*) AS INT) AS total_tests,
    CAST(COUNT(DISTINCT patient_id) AS INT) AS unique_patients_tested,
    CAST(COUNT(DISTINCT doctor_id) AS INT) AS active_doctor_count,
    CAST(SUM(CASE WHEN result_status = 'Abnormal' THEN 1 ELSE 0 END) AS INT) AS abnormal_test_count,
    CAST(SUM(CASE WHEN result_status = 'Critical' THEN 1 ELSE 0 END) AS INT) AS critical_test_count
  FROM adb_caresync.silver.lab_results
  GROUP BY hospital_id, test_date
),
enriched AS (
  SELECT
    dm.hospital_id,
    h.hospital_name,
    dm.summary_date,
    c.day_name,
    c.is_weekend,
    dm.total_tests,
    dm.unique_patients_tested,
    dm.active_doctor_count,
    dm.abnormal_test_count,
    dm.critical_test_count,
    CAST(CASE WHEN dm.total_tests = 0 THEN NULL ELSE ROUND(dm.abnormal_test_count * 100.0 / dm.total_tests, 2) END AS DECIMAL(5,2)) AS abnormal_rate_pct,
    CAST(CASE WHEN dm.total_tests = 0 THEN NULL ELSE ROUND(dm.critical_test_count * 100.0 / dm.total_tests, 2) END AS DECIMAL(5,2)) AS critical_rate_pct
  FROM daily_metrics AS dm
  LEFT JOIN adb_caresync.silver.hospitals AS h
    ON dm.hospital_id = h.hospital_id
  LEFT JOIN adb_caresync.silver.calendar AS c
    ON CAST(date_format(dm.summary_date, 'yyyyMMdd') AS INT) = c.date_key
)
SELECT
  hospital_id,
  hospital_name,
  summary_date,
  day_name,
  is_weekend,
  total_tests,
  unique_patients_tested,
  active_doctor_count,
  abnormal_test_count,
  critical_test_count,
  abnormal_rate_pct,
  critical_rate_pct,
  CAST(LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date) AS INT) AS prev_day_total_tests,
  CAST(
    CASE
      WHEN LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date) IS NULL
        OR LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date) = 0 THEN NULL
      ELSE ROUND(
        (total_tests - LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date)) * 100.0
        / LAG(total_tests) OVER (PARTITION BY hospital_id ORDER BY summary_date),
        2
      )
    END AS DECIMAL(6,2)
  ) AS test_volume_change_pct
FROM enriched
""")

updated_df = spark.table("adb_caresync.gold.hospital_daily_summary")
record_count = updated_df.count()
dbutils.notebook.exit(str(record_count))

# COMMAND ----------

updated_df.show()

# COMMAND ----------

