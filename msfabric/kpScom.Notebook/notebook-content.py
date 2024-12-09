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

from pyspark.sql.functions import max


# Path to Qualys File
        pthToFiles = "Files/raw/qualys"
        tableName = "tblScanDetail"

        # Read the Parquet file into a DataFrame
        print(f"Reading parquet files from {pthToFiles}")
        df = spark.read.parquet(pthToFiles)

        # Check if 'processedDateTime' column exists and retrieve its first value
        if "processedDateTime" in df.columns:
            first_processed_date_time = df.select("processedDateTime").first()[0]
            print("First processedDateTime:", first_processed_date_time)
        else:
            print("Column 'processedDateTime' not found in the Parquet file")
            # Exiting the script if the column is missing
            exit()

        # Load the existing table 'tblScanDetail' from the Lakehouse
        try:
            tbl_scan_detail_df = spark.table("tblscandetail")
        except Exception as e:
            print(f"Error loading table 'tblscandetail': {e}")
            exit()

        # Check if 'dataDate' column exists in tblScanDetail
        if "dataDate" in tbl_scan_detail_df.columns:
            # Get the maximum 'dataDate' value without using alias() incorrectly
            latest_data_date = tbl_scan_detail_df.agg(max("dataDate").alias("latest_data_date")).first()["latest_data_date"]
            print("Latest data date:", latest_data_date)
        else:
            print("Column 'dataDate' not found in 'tblScanDetail'")

        # Compare the dates and decide whether to process new data
        if first_processed_date_time > latest_data_date:
            print("We have new data and need to process it.")
            # Place any further processing logic here
              run_functions_in_parallel(
 
                    lambda: processSrcfilesAllFields("Files/raw/bfGlobalFixlets", "tblBigFixGlobalFixlets"),
                    lambda: processSrcfilesAllFields("Files/raw/bfHardware", "tblBigFixHardware"),
                    lambda: processSrcfilesAllFields("Files/raw/uhc", "tblUhc"),
                    lambda: getVulnSrcSubset(["solution"],"tblVulnExtSolution","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["results"],"tblVulnExtResults","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["impact"],"tblVulnExtImpact","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["diagnosis"],"tblVulnExtDiagnosis","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["product"],"tblVulnExtProduct","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["CVE"],"tblVulnExtCve","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["bugtraq_ids"],"tblVulnExtBugTraq","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["category"],"tblVulnExtBugCategory","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["explt_concat"],"tblVulnExtBugExpltConcat","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["malware_concat"],"tblVulnExtBugMalware","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["vendorReference"],"tblVulnExtVendorReference","Files/raw/qualys"),
                    lambda: getVulnSrcSubset(["vendor"],"tblVulnExtVendor","Files/raw/qualys"),
                    lambda: setUpVulnData(),
                    lambda: getscandetail(),
                    lambda: setUpHostData(),
                    lambda: setUpAtlasAppHardware(),
                    lambda: getKpAnalysisData(),
                    lambda: getJiraData()
                    
                )
            print(f"---------------------------------------------")
            print(f" Bronze Layer completed")
            print(f"---------------------------------------------")

            run_functions_in_parallel(
                lambda: getVersionInfoFromSolutionsAndSaveToLake(),
                lambda: getVersionInfoFromResultsAndSaveToLake(),
                lambda: saveAggAtlasData(),
                lambda: getQualysQidToBigFixFixlet()
                    
            )
            print(f"---------------------------------------------")
            print(f" Silver Layer completed")
            print(f"---------------------------------------------")

            run_functions_in_parallel(
                lambda: get_vuln_add_data_and_save_to_lakehouse(),
                lambda: getGoldTblHost()
                    
            )

            run_functions_in_parallel(
                lambda: explode_data_column("g_tblvuln","uuid_hash","qualysCve","lkupVulnCve"),
                lambda: explode_data_column("g_tblvuln","uuid_hash","fixletId","lkupVulnFixletId"),
                lambda: explode_data_column("g_tblvuln","uuid_hash","kpAnalysis_Group","lkupVulnAnalysisGroup"),
                lambda: explode_data_column("g_tblvuln","uuid_hash","jiraNumbers","lkupVulnJiraNumbers"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasAppId","lkupHostAtlasAppId"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasApplicationName","lkupHostAtlasApplicationName"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasApplicationManager","lkupHostAtlasApplicationManager"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasEnvironment","lkupHostAtlasEnvironment"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasOwningOrgLevel1Executive","lkupHostAtlasLevel1Executive"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasOwningOrgLevel2Executive","lkupHostAtlasLevel2Executive"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasOwningOrgLevel3Executive","lkupHostAtlasLevel3Executive"),
                lambda: explode_data_column("g_tblhost","uuid_hash","atlasOwningOrgLevel4Executive","lkupHostAtlasLevel4Executive")
            )

            print(f"---------------------------------------------")
            print(f" Gold Layer completed")
            print(f"---------------------------------------------")


        else:

            print("No new data. Exiting.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
