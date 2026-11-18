from pyspark import pipelines as dp
from pyspark.sql import functions as F

GROUP_BY_COLS = ["NPI", "Ind_PAC_ID", "Ind_enrl_ID"]

# Source column -> output alias, taking the latest value by ingestion_timestamp
LATEST_COLS = {
    "Provider_Last_Name": "last_name",
    "Provider_First_Name": "first_name",
    "gndr": "gender",
    "Cred": "credential",
    "Med_sch": "Med_sch",
    "Grd_yr": "Grd_yr",
    "pri_spec": "pri_spec",
    "Facility_Name": "Facility_Name",
    "adr_ln_1": "Fac_addr",
    "City/Town": "Fac_city",
    "State": "Fac_state",
    "ZIP_Code": "Fac_zip",
}

@dp.materialized_view(name="northstar_catalog_silver.cms_prov_comp_silver.prov_comp_mv")
def prov_num_org_member():
    aggs = [
        F.sum(F.expr("TRY_CAST(num_org_mem AS BIGINT)")).alias("num_org_mem"),
        F.count("*").alias("NPI_row_count"),
    ]
    aggs += [
        F.max_by(col, "ingestion_timestamp").alias(alias)
        for col, alias in LATEST_COLS.items()
    ]
    return (
        spark.read.table("northstar_network_bronze.cms_provider_compare.cms_prov_comp_ingest_data")
        .groupBy(GROUP_BY_COLS)
        .agg(*aggs)
    )    