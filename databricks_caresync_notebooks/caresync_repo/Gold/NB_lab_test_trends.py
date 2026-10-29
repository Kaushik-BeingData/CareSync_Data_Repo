# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Build gold lab_test_trends table
spark.sql("CREATE SCHEMA IF NOT EXISTS adb_caresync.gold")

spark.sql("""
 CREATE OR REPLACE TABLE adb_caresync.gold.lab_test_trends AS
 WITH monthly_metrics AS (
   SELECT
     test_name,
     year(test_date) AS year,
     month(test_date) AS month,
     date_trunc('month', test_date) AS month_start_date,
     CAST(COUNT(*) AS INT) AS total_tests,
     CAST(SUM(CASE WHEN result_status = 'Abnormal' THEN 1 ELSE 0 END) AS INT) AS abnormal_test_count,
     CAST(SUM(CASE WHEN result_status = 'Critical' THEN 1 ELSE 0 END) AS INT) AS critical_test_count
   FROM adb_caresync.silver.lab_results
   GROUP BY test_name, year(test_date), month(test_date), date_trunc('month', test_date)
 ),
 enriched AS (
   SELECT
     mm.test_name,
     mm.year,
     mm.month,
     COALESCE(c.month_name, date_format(mm.month_start_date, 'MMMM')) AS month_name,
     CONCAT('Q', quarter(mm.month_start_date)) AS quarter_name,
     mm.total_tests,
     mm.abnormal_test_count,
     mm.critical_test_count,
     CAST(CASE WHEN mm.total_tests = 0 THEN NULL ELSE ROUND(mm.abnormal_test_count * 100.0 / mm.total_tests, 2) END AS DECIMAL(5,2)) AS abnormal_rate_pct,
     CAST(CASE WHEN mm.total_tests = 0 THEN NULL ELSE ROUND(mm.critical_test_count * 100.0 / mm.total_tests, 2) END AS DECIMAL(5,2)) AS critical_rate_pct
   FROM monthly_metrics AS mm
   LEFT JOIN adb_caresync.silver.calendar AS c
     ON CAST(date_format(mm.month_start_date, 'yyyyMMdd') AS INT) = c.date_key
 )
 SELECT
   test_name,
   year,
   month,
   month_name,
   quarter_name,
   total_tests,
   abnormal_test_count,
   critical_test_count,
   abnormal_rate_pct,
   critical_rate_pct,
   CAST(LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month) AS DECIMAL(5,2)) AS prev_month_abnormal_rate_pct,
   CAST(
     CASE
       WHEN LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month) IS NULL THEN NULL
       ELSE ROUND(
         abnormal_rate_pct - LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month),
         2
       )
     END AS DECIMAL(6,2)
   ) AS abnormal_rate_change_pct
 FROM enriched
""")

updated_df = spark.table("adb_caresync.gold.lab_test_trends")
record_count = updated_df.count()
dbutils.notebook.exit(str(record_count))

# COMMAND ----------

updated_df.display()

# COMMAND ----------

