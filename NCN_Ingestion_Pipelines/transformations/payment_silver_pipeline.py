from pyspark import pipelines as dp
from pyspark.sql import functions as F

Bronze_table = "northstar_network_bronze.payment_data_bronze.payment_data_bronze"
silver_schema_path = "northstar_catalog_silver.payment_data_silver"

rules = {
    "valid_payment": "Total_Payment_Amt IS NOT NULL",
    "Has_specialty": "doctor_Specialty IS NOT NULL",
    "Has_NPI": "doctor_NPI IS NOT NULL",
}

@dp.table(name=f"{silver_schema_path}.payment_data_silver")
@dp.expect_all_or_drop(rules)
def payment_data_silver():
    # read the zip code data only the county name column and not null values
    zip_code_data = spark.sql("""SELECT GEOID_ZCTA5_20, NAMELSAD_COUNTY_20
                                FROM northstar_network_bronze.zcta_2020_dta.zcta_2020_bronze
                                WHERE GEOID_ZCTA5_20 IS NOT NULL""")
    return (
        spark.readStream
        .table(Bronze_table)
        .select(
            F.col("Covered_Recipient_Type").alias("doctor_Type"),
            F.col("Covered_Recipient_Profile_ID").alias("doctor_profile_ID"),
            F.col("Covered_Recipient_NPI").alias("doctor_NPI"),
            F.col("Covered_Recipient_Specialty_1").alias("doctor_Specialty"),
            F.col("Total_Amount_of_Payment_USDollars").alias("Total_Payment_Amt").cast("double"),
            F.col("Covered_Recipient_First_Name").alias("doctor_First_Name"),
            F.col("Covered_Recipient_Last_Name").alias("doctor_Last_Name"),
            F.col("Recipient_Primary_Business_Street_Address_Line1").alias("Bus_Address_Line1"),
            F.col("Recipient_Primary_Business_Street_Address_Line2").alias("Bus_Address_Line2"),
            F.col("Recipient_City").alias("Bus_City"),
            F.col("Recipient_State").alias("Bus_State"),
            F.col("Recipient_Zip_Code").alias("Bus_Zip"),
            F.col("Covered_Recipient_Primary_Type_1").alias("doctor_Primary_type"),
            F.col("Covered_Recipient_License_State_code1").alias("doctor_License_State"),
            F.col("Date_of_Payment").alias("Date_of_Payment").cast("date"),
            F.col("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name").alias("Submitted_BY_Name"),
            F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID").alias("Payment_ID"),
            F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name").alias("Payer_name"),
            F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State").alias("Payer_State"),
            F.col("Date_of_Payment").alias("payment_date"),
            F.col("Number_of_Payments_Included_in_Total_Amount").alias("Number_of_Payments").cast("int"),
            F.col("Form_of_Payment_or_Transfer_of_Value").alias("Form_of_Payment"),
            F.col("Nature_of_Payment_or_Transfer_of_Value").alias("Nature_of_Payment"),
            F.col("City_of_Travel"), F.col("State_of_Travel"), F.col("Country_of_Travel"),
            F.col("Physician_Ownership_Indicator").alias("Physician_Ownership"),
            F.col("Third_Party_Payment_Recipient_Indicator").alias("Third_party_Payer"),
            F.col("Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value").alias("Third_Payer_Name"),
            F.col("Charity_Indicator").alias("Charity"), F.col("Record_ID"), 
            F.col("Related_Product_Indicator").alias("Related_Product"),
            F.col("Covered_or_Noncovered_Indicator_1").alias("Covered_or_Noncovered"),
            F.col("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1").alias("Product"),
            F.col("Product_Category_or_Therapeutic_Area_1").alias("Product_Category"),
            F.col("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1").alias("Product_specifier"),
            F.col("Program_Year"), F.col("Payment_Publication_Date")
        )
        .join(zip_code_data, F.col("Bus_Zip") == F.col("GEOID_ZCTA5_20"), "left")
        .withColumn("County", F.col("NAMELSAD_COUNTY_20"))
        .drop("GEOID_ZCTA5_20", "NAMELSAD_COUNTY_20")
    )


@dp.table(name=f"{silver_schema_path}.payment_data_quarantine")
@dp.expect_all_or_drop({
    "valid_payment": "Total_Payment_Amt IS NULL",
    "Has_specialty": "doctor_Specialty IS NULL",
    "Has_NPI": "doctor_NPI IS NULL"
})
def payment_data_quarantine():
    return (
        spark.readStream
        .table(Bronze_table)
        .select(
            F.col("Covered_Recipient_Type").alias("doctor_Type"),
            F.col("Covered_Recipient_Profile_ID").alias("doctor_profile_ID"),
            F.col("Covered_Recipient_NPI").alias("doctor_NPI"),
            F.col("Covered_Recipient_Specialty_1").alias("doctor_Specialty"),
            F.col("Total_Amount_of_Payment_USDollars").alias("Total_Payment_Amt").cast("double"),
            F.col("Covered_Recipient_First_Name").alias("doctor_First_Name"),
            F.col("Covered_Recipient_Last_Name").alias("doctor_Last_Name"),
            F.col("Recipient_Primary_Business_Street_Address_Line1").alias("Bus_Address_Line1"),
            F.col("Recipient_Primary_Business_Street_Address_Line2").alias("Bus_Address_Line2"),
            F.col("Recipient_City").alias("Bus_City"),
            F.col("Recipient_State").alias("Bus_State"),
            F.col("Recipient_Zip_Code").alias("Bus_Zip"),
            F.col("Covered_Recipient_Primary_Type_1").alias("doctor_Primary_type"),
            F.col("Covered_Recipient_License_State_code1").alias("doctor_License_State"),
            F.col("Date_of_Payment").alias("Date_of_Payment").cast("date"),
            F.col("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name").alias("Submitted_BY_Name"),
            F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID").alias("Payment_ID"),
            F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name").alias("Payer_name"),
            F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State").alias("Payer_State"),
            F.col("Date_of_Payment").alias("payment_date"),
            F.col("Number_of_Payments_Included_in_Total_Amount").alias("Number_of_Payments").cast("int"),
            F.col("Form_of_Payment_or_Transfer_of_Value").alias("Form_of_Payment"),
            F.col("Nature_of_Payment_or_Transfer_of_Value").alias("Nature_of_Payment"),
            F.col("City_of_Travel"), F.col("State_of_Travel"), F.col("Country_of_Travel"),
            F.col("Physician_Ownership_Indicator").alias("Physician_Ownership"),
            F.col("Third_Party_Payment_Recipient_Indicator").alias("Third_party_Payer"),
            F.col("Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value").alias("Third_Payer_Name"),
            F.col("Charity_Indicator").alias("Charity"), F.col("Record_ID"), 
            F.col("Related_Product_Indicator").alias("Related_Product"),
            F.col("Covered_or_Noncovered_Indicator_1").alias("Covered_or_Noncovered"),
            F.col("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1").alias("Product"),
            F.col("Product_Category_or_Therapeutic_Area_1").alias("Product_Category"),
            F.col("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1").alias("Product_specifier"),
            F.col("Program_Year"), F.col("Payment_Publication_Date")
        )
    )






# Quarantine table create