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
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
