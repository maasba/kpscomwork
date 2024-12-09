# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ff1ed9d9-4400-44ed-bf3f-255cec85cc0d",
# META       "default_lakehouse_name": "lhKpScom",
# META       "default_lakehouse_workspace_id": "ace43e1b-866a-4e00-a24b-b9997ff19815",
# META       "known_lakehouses": [
# META         {
# META           "id": "ff1ed9d9-4400-44ed-bf3f-255cec85cc0d"
# META         },
# META         {
# META           "id": "874f0bb1-afc4-4cac-85aa-668087157a6a"
# META         },
# META         {
# META           "id": "83d475f1-10ea-4aab-8901-2c2383abfdec"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run ./utilFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./00_Silver_ProcessSources

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./01_Silver_ProcessSources

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
  # Perform actions if the table exists
print("The table exists. Proceeding with further operations.")

# Get the latest data dates
latest_data_date_from_data_in_repo = spark.table("tblscandetail") \
.agg(max("dataDate").alias("latest_data_date")) \
.first()["latest_data_date"]


dfScanDetail =spark.table("tblscandetail")

update_dataDateStatusWithDefaultValues()

data_date_status = dfScanDetail \
    .filter(dfScanDetail["dataDate"] == latest_data_date_from_data_in_repo) \
    .select("dataDateStatus") \
    .first()["dataDateStatus"]

print(f"Latest Data date : {data_date_status}")
update_column_value_in_table("tblscandetail", "dataDateStatus", data_date_status, "Latest")
  



    displayDataFrame(df)
"""
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lower, when

from pyspark.sql.functions import col, lower, regexp_replace, when

# Path to the files
pthToFiles = "Files/raw/bfGlobalFixlets/bfGlobalFixlets"

# Read the parquet file
df = spark.read.parquet(pthToFiles)

# Preprocess the `name` column to remove irrelevant substrings when determining `oscategory`
df = df.withColumn(
    "processed_name",
    regexp_replace(lower(col("name")), "(x86|x64|x32|\\s+-\\s+.*)", "")  # Remove architectures and unrelated text after "-"
)

# Define the logic for the oscategory column based on the processed name
df = df.withColumn(
    "oscategory",
    when(
        lower(col("processed_name")).like("%win%"),
        "windows"
    ).when(
        lower(col("processed_name")).like("%redhat%") | lower(col("processed_name")).like("%rhel%"), "redhat9")
    ).when(
        lower(col("processed_name")).like("%redhat%") | lower(col("processed_name")).like("%rhel%"),
        when(lower(col("processed_name")).like("%6%"), "redhat6")
        .when(lower(col("processed_name")).like("%7%"), "redhat7")
        .when(lower(col("processed_name")).like("%8%"), "redhat8")
        .when(lower(col("processed_name")).like("%9%"), "redhat9")
        .otherwise("redhat")
    ).otherwise("other")  # Default case for unknown oscategory
)

# Drop the processed_name column to ensure it is not used further
df = df.drop("processed_name")

# Select only the name and oscategory columns
result_df = df.select("name", "oscategory")

# Filter the DataFrame to only include rows where oscategory is like 'redhat%' or 'centos%'
filtered_df = result_df.filter(lower(col("oscategory")).like("redhat%"))

# Display the filtered DataFrame
displayDataFrame(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
