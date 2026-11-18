-- Databricks notebook source
DESCRIBE HISTORY northstar_network_bronze.payment_data_bronze.payment_data_bronze LIMIT 5

-- COMMAND ----------

select * from _sqldf

-- COMMAND ----------

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

SELECT COUNT(*)
FROM northstar_catalog_silver.payment_data_silver.payment_data_silver

-- COMMAND ----------

SELECT COUNT(*) 
FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze 

-- COMMAND ----------

DESCRIBE EXTENDED northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table

-- COMMAND ----------

SELECT * FROM system.billing.usage

-- COMMAND ----------

SELECT SPECIALTY, COUNT(*) 
FROM northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table
GROUP BY SPECIALTY
ORDER BY COUNT(*) DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count = spark.table("northstar_catalog_silver.exclude_indiv_silver.exclude_indiv_silver").select("SPECIALTY").distinct().count()
-- MAGIC expected_count = spark.table("northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table").select("SPECIALTY").distinct().count() - 4
-- MAGIC print(expected_count, count)
-- MAGIC
-- MAGIC assert count == expected_count
-- MAGIC print("Unit test passed.")

-- COMMAND ----------

SELECT * 
FROM northstar_network_bronze.cms_provider_compare.cms_prov_comp_ingest_data

-- COMMAND ----------

SELECT count(*)
FROM 
    (SELECT  cpcid.NPI, cpcid.Ind_PAC_ID, cpcid.Ind_enrl_ID, COUNT(*) AS COUNT
    FROM northstar_network_bronze.cms_provider_compare.cms_prov_comp_ingest_data AS cpcid
    GROUP BY cpcid.NPI, cpcid.Ind_PAC_ID, cpcid.Ind_enrl_ID) AS t


-- COMMAND ----------

SELECT COUNT(*)
FROM northstar_catalog_silver.cms_prov_comp_silver.prov_comp_mv

-- COMMAND ----------

SELECT COUNT(*) AS total_count, COUNT(DISTINCT(NPI)) AS total_npi
FROM northstar_network_bronze.cms_provider_compare.cms_prov_comp_ingest_data


-- COMMAND ----------

SELECT
  COUNT(*) AS total,
  COUNT_IF(COALESCE(TRY_CAST(NPI AS BIGINT), 0) >  0) AS with_npi,
  COUNT_IF(COALESCE(TRY_CAST(NPI AS BIGINT), 0) =  0) AS without_npi,
  COUNT_IF(NPI IS NULL) AS with_npi_null
FROM northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table
WHERE SPECIALTY = "CLERK/OTHER CLERICAL";

-- COMMAND ----------

SELECT SUM(t.county_count)
FROM (SELECT Bus_Zip, COUNT(DISTINCT(County)) AS county_count
        FROM northstar_catalog_silver.payment_data_silver.payment_data_silver
        GROUP BY Bus_Zip
        HAVING COUNT(DISTINCT(County)) > 1) as t


-- COMMAND ----------

SELECT count(*) AS total_count,
    (SELECT COUNT(*) 
    FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze           
    WHERE try_cast(Total_Amount_of_Payment_USDollars AS FLOAT) > 0 ) AS count_positive_payment,

    (SELECT COUNT(*) 
    FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze           
    WHERE try_cast(Total_Amount_of_Payment_USDollars AS FLOAT) = 0 ) AS count_zero_payment,

    (SELECT count(*) 
    FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze           --28 null payments
    WHERE Total_Amount_of_Payment_USDollars IS NULL) AS null_payment
FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze;           


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

select *
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

-- COMMAND ----------

SET TAG ON TABLE workspace.bookstore_etl_pro.bronze project = bookstore;

-- COMMAND ----------

SELECT * FROM northstar_catalog_silver.payment_data_silver.payment_data_silver_profile_metrics