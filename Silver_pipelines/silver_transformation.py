from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER = "northstar_catalog_silver.cms_provider_api_silver"   
SRC = "northstar_network_bronze.cms_provider_specialty.api_ingestion_table"

DOUBLE_COLS = [
    "Avg_Mdcr_Alowd_Amt", "Avg_Mdcr_Pymt_Amt", "Avg_Mdcr_Stdzd_Amt",
    "Avg_Sbmtd_Chrg", "Tot_Bene_Day_Srvcs", "Tot_Benes", "Tot_Srvcs",
]

@dp.temporary_view(name="providers_typed")
@dp.expect_all_or_drop({
    "positive_allowed_amt": "Avg_Mdcr_Alowd_Amt > 0",
    "positive_payment_amt": "Avg_Mdcr_Pymt_Amt > 0",
    "positive_standard_amt": "Avg_Mdcr_Stdzd_Amt > 0",
})
def providers_typed():
    df = spark.readStream.table(SRC)
    for c in DOUBLE_COLS:
        df = df.withColumn(c, F.expr(f"try_cast({c} AS DOUBLE)"))
    return (
        df.withColumn("_ingest_date", F.col("_ingest_date").cast("date"))
          .withColumn("_ingest_time", F.col("_ingest_time").cast("timestamp"))
    )

dp.create_streaming_table(
    name=f"{SILVER}.silver_providers",
    schema="""
        Avg_Mdcr_Alowd_Amt DOUBLE, Avg_Mdcr_Pymt_Amt DOUBLE, Avg_Mdcr_Stdzd_Amt DOUBLE,
        Avg_Sbmtd_Chrg DOUBLE, HCPCS_Cd STRING, HCPCS_Desc STRING, HCPCS_Drug_Ind STRING,
        Place_Of_Srvc STRING, Rndrng_NPI STRING, Rndrng_Prvdr_City STRING,
        Rndrng_Prvdr_Cntry STRING, Rndrng_Prvdr_Crdntls STRING, Rndrng_Prvdr_Ent_Cd STRING,
        Rndrng_Prvdr_First_Name STRING, Rndrng_Prvdr_Last_Org_Name STRING,
        Rndrng_Prvdr_MI STRING, Rndrng_Prvdr_Mdcr_Prtcptg_Ind STRING,
        Rndrng_Prvdr_RUCA STRING, Rndrng_Prvdr_RUCA_Desc STRING, Rndrng_Prvdr_St1 STRING,
        Rndrng_Prvdr_St2 STRING, Rndrng_Prvdr_State_Abrvtn STRING,
        Rndrng_Prvdr_State_FIPS STRING, Rndrng_Prvdr_Type STRING, Rndrng_Prvdr_Zip5 STRING,
        Tot_Bene_Day_Srvcs DOUBLE, Tot_Benes DOUBLE, Tot_Srvcs DOUBLE,
        _ingest_date DATE, _ingest_time TIMESTAMP,
        __START_AT TIMESTAMP, __END_AT TIMESTAMP
    """,
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.silver_providers",
    source="providers_typed",
    keys=["Rndrng_NPI", "HCPCS_Cd", "Place_Of_Srvc"],
    sequence_by="_ingest_time",
    stored_as_scd_type=2,
)