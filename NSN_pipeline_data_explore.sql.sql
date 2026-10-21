-- Databricks notebook source
SELECT COUNT(*)
FROM northstar_network_bronze.payment_data_bronze.payment_data_bronze
-- LIMIT 100;

-- COMMAND ----------

SELECT COUNT(*)
FROM northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC select count(*) from northstar_network_bronze.cms_provider_compare.cms_prov_comp_ingest_data;
-- MAGIC -- limit 100

-- COMMAND ----------

SELECT COUNT(*)
FROM northstar_network_bronze.nppes_data.npi_bronze;

-- COMMAND ----------

ALTER TABLE northstar_network_bronze.nppes_data.npi_bronze
SET TBLPROPERTIES (delta.appendOnly = true);

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