-- Databricks notebook source
SELECT 
    min(A.Total_Amount_of_Payment_USDollars) AS minimum_payment, 
    max(A.Total_Amount_of_Payment_USDollars) AS maximum_payment, 
    (SELECT count(*) 
    FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze           --28 null payments
    WHERE Total_Amount_of_Payment_USDollars IS NULL) AS null_payment,
    min(A.Date_of_Payment) as earliest_payment_date,                                    --1/1/2025
    (SELECT count(*) 
    FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze
    WHERE Covered_Recipient_Specialty_1 IS NULL) AS null_specialty,                     --14 null specialty
    (SELECT count(*) 
    FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze
    WHERE Covered_Recipient_NPI IS NULL) AS null_npi                --299 null npi
FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze as A

-- COMMAND ----------

SELECT
    min(try_cast(A.Avg_Mdcr_Alowd_Amt AS DOUBLE)) as min_avg_mdcr_alowd_amt,
    max(try_cast(A.Avg_Mdcr_Alowd_Amt AS DOUBLE)) as max_avg_mdcr_alowd_amt,
    min(try_cast(A.Avg_Mdcr_Pymt_Amt AS DOUBLE)) as min_avg_mdcr_pymt_amt,
    max(try_cast(A.Avg_Mdcr_Pymt_Amt AS DOUBLE)) as max_avg_mdcr_pymt_amt,
    min(try_cast(A.Avg_Mdcr_Stdzd_Amt AS DOUBLE)) as min_avg_mdcr_stdzd_amt,
    max(try_cast(A.Avg_Mdcr_Stdzd_Amt AS DOUBLE)) as max_avg_mdcr_stdzd_amt,
    min(try_cast(A.Avg_Sbmtd_Chrg AS DOUBLE)) as min_avg_sbmtd_chrg,
    max(try_cast(A.Avg_Sbmtd_Chrg AS DOUBLE)) as max_avg_sbmtd_chrg,
    (SELECT COUNT(*)
    FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table
    WHERE Avg_Mdcr_Alowd_Amt IS NULL) as null_avg_mdcr_alowd_amt,       --2
    (SELECT COUNT(*)
    FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table
    WHERE Avg_Mdcr_Pymt_Amt IS NULL) as null_avg_mdcr_pymt_amt,         --18
    (SELECT COUNT(*)
    FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table
    WHERE Avg_Mdcr_Stdzd_Amt IS NULL) as null_avg_mdcr_stdzd_amt,   --13
    (SELECT COUNT(*)
    FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table
    WHERE Avg_Sbmtd_Chrg IS NULL) as null_avg_sbmtd_chrg                --0
FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table as A

-- COMMAND ----------

DESCRIBE EXTENDED northstar_network_bronze.cms_provider_specialty.api_ingestion_table

-- COMMAND ----------

SELECT *
FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table as A
LIMIT 100;

-- COMMAND ----------

SELECT COUNT(*)
-- SELECT * 
FROM northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table AS A
-- WHERE A.NPI = '0';

-- COMMAND ----------

-- DBTITLE 1,Cell 5
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC df = spark.table("northstar_network_bronze.nppes_data.npi_bronze")
-- MAGIC sdf = df.select([F.col(f"`{c}").alias(c.replace(".", "_")) for c in df.columns]).summary()
-- MAGIC display(sdf)

-- COMMAND ----------

DESCRIBE EXTENDED northstar_network_bronze.nppes_data.npi_bronze
-- FROM northstar_network_bronze.nppes_data.npi_bronze
-- LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Set the streaming table property to append only.

-- COMMAND ----------

SHOW TBLPROPERTIES northstar_network_bronze.nppes_data.npi_bronze;

-- COMMAND ----------

SELECT COUNT(*) 
FROM read_files(
  '/Volumes/northstar_network_bronze/nppes_data/nppes_data_storage/part_aa.csv',
  format => 'csv',
  header => true
)


-- COMMAND ----------

select count(*)
from northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table

-- COMMAND ----------

SELECT * 
FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze
WHERE ingestion_timestamp > '2026-10-17 19:54:12'
LIMIT 100

-- COMMAND ----------

ALTER TABLE northstar_network_bronze.payment_data_bronze.__materialization_mat_e446221f_8b32_4c7e_88bc_ec717002f7f2_payment_data_bronze_1
SET TBLPROPERTIES (delta.appendOnly = false);

-- COMMAND ----------

SELECT * 
FROM northstar_network_bronze.cms_provider_specialty.api_ingestion_table as A
-- GROUP BY A.Rndrng_Prvdr_Type;