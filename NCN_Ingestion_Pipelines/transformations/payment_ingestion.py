from pyspark import pipelines as dp
from pyspark.sql import functions as F


volume_path = spark.conf.get("payment_data_volume")

@dp.table(
    name="payment_data_bronze",
    partition_cols=["Covered_Recipient_Specialty_1"],
    table_properties={
        "delta.appendOnly": "false",
        "pipelines.reset.allowed": "true"
    }
)

def payment_data_bronze():
                    
    payment_bronze_df = (                                        # let the pipeline infer the schema itself
        spark.readStream.format("cloudFiles")       # cloudFiles documentation - https://docs.databricks.com/aws/en/ldp/load#load-from-cloud
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")            # schema evolution
        .load(f"{volume_path}/nsn_payment_data")          #volume path
        .withColumn("Covered_Recipient_Profile_ID",F.trim(F.col("Covered_Recipient_Profile_ID")).cast("bigint"))
        .withColumn("Covered_Recipient_NPI",F.trim(F.col("Covered_Recipient_NPI")).cast("bigint"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("source_file_name", F.col("_metadata.file_name"))
    )

    return payment_bronze_df
        

