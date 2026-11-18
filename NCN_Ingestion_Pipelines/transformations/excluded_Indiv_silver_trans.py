from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema_path = "northstar_catalog_silver.exclude_indiv_silver"

DELETE_SPECIALTIES = ['SALES/MARKET/RETAIL', 'ACCOUNT/BOOKKPR/AUDI', 'RELATED', 'CLERK/OTHER CLERICAL']

dp.create_streaming_table(name=f"{silver_schema_path}.exclude_indiv_silver")

dp.create_auto_cdc_flow(
    target=f"{silver_schema_path}.exclude_indiv_silver",
    source="northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table",
    keys=['NPI', 'DOB', 'EXCLDATE'],
    sequence_by='ingestion_timestamp',
    apply_as_deletes=F.col('SPECIALTY').isin(DELETE_SPECIALTIES),
    stored_as_scd_type=1
)