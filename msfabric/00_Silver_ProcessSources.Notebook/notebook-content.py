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

# MARKDOWN ********************

# #### Upload Jira data

# CELL ********************

def getJiraData():
    # Path to the CSV file
    pth_to_files = "Files/manualUpload/jiraOverall.csv"

    # Read the CSV file into a PySpark DataFrame
    dfJiraAll = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", "^") \
        .option("quote", '"') \
        .option("multiline", "true") \
        .csv(pth_to_files)


    # Filter the DataFrame based on Issue Type
    dfTreatments = dfJiraAll.filter(col("Issue Type") == "Story")
    dfQid = dfJiraAll.filter(col("Issue Type") == "Sub-task")



    # Select only necessary columns from dfQid
    dfQid = dfQid.select("Summary", "Parent id", "Issue key")
    qid_regex = r"(?i)\b(QID|QUID)\D*(\d+)"  # \b ensures a word boundary, \D* skips non-digits, \d+ captures the number

    # Extract the numeric value after QID or QUID
    dfQid = dfQid.withColumn("Qid", regexp_extract(col("Summary"), qid_regex, 2))


    # Rename conflicting columns in dfQid to avoid ambiguity during the join
    dfQid = dfQid.withColumnRenamed("Parent id", "ParentIssueId").withColumnRenamed("Issue key", "QidKey").withColumnRenamed("Summary", "QidSummary")

    # Perform the join between dfTreatments and dfQid
    merged_df = dfTreatments.join(
        dfQid,
        dfTreatments["Issue id"] == dfQid["ParentIssueId"],
        how="left"
    )

    # Select only unique and necessary columns from the resulting DataFrame
    selected_columns_df = merged_df.select(
        col("Issue key").alias("jiraNumber"),
        col("Created").alias("createDate"),
        col("Updated").alias("lastUpdated"),
        col("Issue Type").alias("jiraType"),
        col("Status").alias("treatmentstatus"),
        col("Fix Version/s").alias("treatmentReleaseVersion"),
        col("Summary").alias("treatmentSummary"),
        col("Qid"),
        col("QidSummary").alias("QidDescription")
        
    )

    # Convert the PySpark DataFrame to a Pandas DataFrame
    final_pandas_df = selected_columns_df
    create_table_with_id(final_pandas_df,"tblJira")
    #display(final_pandas_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Upload the files from tf Analysis team source

# MARKDOWN ********************

# #### Set up Scan Detail

# CELL ********************


def getscandetail():
    pthToFiles = "Files/raw/qualys"
    tableName = "tblScanDetail"
    
    # Read the Parquet file into a DataFrame
    print(f"Reading parquet files from {pthToFiles}")
    scandetaildf = spark.read.parquet(pthToFiles).cache() 
    
    device_type_filter = (col("deviceType") == "Server") & \
                        (coalesce(col("NBName"), col("DNSName")).isNotNull())
    scandetaildf = scandetaildf.filter(device_type_filter)

    # Cache the filtered DataFrame as it will be reused
    scandetaildf.cache()
    
    # Define the columns to select
    allcolumns = [
        "processedDateTime", "vulnFirstFoundDateTime",
        "vulnLastFixedDateTime", "vulnLastFoundDateTime", "assetLastFoundDateTime",
        "vulnPublishedDate", "patchAvailable", "days_open", "days_to_close", "severity",
        "baseScoreValue", "exploitAvailable", "scanType", 
        "vulnFirstFoundLast90Days", "AFFECT_RUNNING_KERNEL",
        "DC_Host", "OSName", "NBName", "DNSName", "IPAddressStr", 
        "authStatus", "authenticationErrorDescription", "site_id", 
        "region", "location", "building", "managed", "HIPAA", 
        "category","explt_concat","malware_concat","bugtraq_ids",
        "PCI", "SOX", "ipInteger", "DMZ_Host", "deviceType", "network_id","qid","title","category","OS_App","cvss_temporal","cvss_v3_base","cvss_v3_temporal",
        "CVE","diagnosis","impact","product","results","solution","vendor","vendorreference","status","protocol","TYPE","riskRating"
    ]

    qid_columns = ["qid","title","category","OS_App","cvss_temporal","cvss_v3_base","cvss_v3_temporal"]
    bugCategory_columns = ["category"]
    explt_concat_columns = ["explt_concat"]
    malware_concat_columns = ["malware_concat"]
    bugtraq_ids_columns = ["bugtraq_ids"]
    cve_columns = ["CVE"]
    diagnosis_columns = ["diagnosis"]
    impact_columns = ["impact"]
    product_columns = ["product"]
    results_columns = ["results"]
    solution_columns = ["solution"]
    vendor_columns = ["vendor"]
    vendorreference_columns = ["vendorreference"]
    hostcolumns = ["DC_Host", "OSName", "NBName", "DNSName", "IPAddressStr", 
            "authStatus", "authenticationErrorDescription", "site_id", 
            "region", "location", "building", "managed", "HIPAA", 
            "PCI", "SOX", "ipInteger", "DMZ_Host", "deviceType", "network_id"]

    


    # Define the list of columns
    scanDetailColumns = [
        "vulnFirstFoundDateTime", 
        "vulnLastFixedDateTime", 
        "vulnLastFoundDateTime",
        "assetLastFoundDateTime", 
        "vulnPublishedDate", 
        "patchAvailable", 
        "days_open", 
        "days_to_close", 
        "severity", 
        "baseScoreValue", 
        "exploitAvailable",
        "scanType", 
        "vulnFirstFoundLast90Days",
        "protocol",
        "TYPE", 
        "riskRating", 
        "status",
        "AFFECT_RUNNING_KERNEL"
    ]

    scandetaildf_with_uuid = scandetaildf
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "scanDetail_hash",
        uuid_udf(*[col(column) for column in scanDetailColumns])  # Pass scanDetailColumns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "vendorref_hash",
        uuid_udf(*[col(column) for column in vendorreference_columns])  # Pass vendorreference_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "qid_hash",
        uuid_udf(*[col(column) for column in qid_columns])  # Pass qid_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "bugCategory_hash",
        uuid_udf(*[col(column) for column in bugCategory_columns])  # Pass bugCategory_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "explt_concat_hash",
        uuid_udf(*[col(column) for column in explt_concat_columns])  # Pass explt_concat_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "malware_concat_hash",
        uuid_udf(*[col(column) for column in malware_concat_columns])  # Pass malware_concat_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "bugtraq_ids_hash",
        uuid_udf(*[col(column) for column in bugtraq_ids_columns])  # Pass bugtraq_ids_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "cve_hash",
        uuid_udf(*[col(column) for column in cve_columns])  # Pass cve_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "diagnosis_hash",
        uuid_udf(*[col(column) for column in diagnosis_columns])  # Pass diagnosis_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "impact_hash",
        uuid_udf(*[col(column) for column in impact_columns])  # Pass impact_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "product_hash",
        uuid_udf(*[col(column) for column in product_columns])  # Pass product_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "results_hash",
        uuid_udf(*[col(column) for column in results_columns])  # Pass results_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "solution_hash",
        uuid_udf(*[col(column) for column in solution_columns])  # Pass solution_columns
    )
    scandetaildf_with_uuid = scandetaildf_with_uuid.withColumn(
        "vendor_hash",
        uuid_udf(*[col(column) for column in vendor_columns])  # Pass vendor_columns
    )

    host_df = (
        scandetaildf_with_uuid
        .groupBy(
            split(coalesce(col("NBName"), col("DNSName")), "\\.")[0].alias("hostname"), "scanDetail_hash"
        )
        .agg(
            *[
                concat_ws(", ", sort_array(collect_set(col(col_name)))).alias(col_name)
                for col_name in hostcolumns if col_name in scandetaildf_with_uuid.columns
            ]
        )
    )

    columns_to_exclude = ["scanDetail_hash"] + scanDetailColumns
    # Exclude `vuln_hash` when adding UUID
    host_df_with_uuid = add_uuid_column(host_df, exclude_columns=columns_to_exclude)

    host_df_with_uuid=host_df_with_uuid.select("scanDetail_hash","uuid_hash") \
            .withColumnRenamed("uuid_hash", "host_hash") 
    #displayDataFrame(host_df_with_uuid)

    # Select all individual columns and the generated UUID hashes
    final_df = scandetaildf_with_uuid.select(
        col("processedDateTime").alias("dataDate"),  # Include processedDateTime as dataDate
        col("scanDetail_hash"),  # Include the UUID column for scanDetail
        col("vendorref_hash"),  # Include the UUID column for vendor reference
        col("qid_hash"),  # Include the UUID column for QID
        col("bugCategory_hash"),  # Include the UUID column for bugCategory
        col("explt_concat_hash"),  # Include the UUID column for explt_concat

        col("malware_concat_hash"),  # Include the UUID column for malware_concat

        col("bugtraq_ids_hash"),  # Include the UUID column for bugtraq_ids
        col("cve_hash"),  # Include the UUID column for CVE
        col("diagnosis_hash"),  # Include the UUID column for diagnosis
        col("impact_hash"),  # Include the UUID column for impact
        col("product_hash"),  # Include the UUID column for product
        col("results_hash"),  # Include the UUID column for results
        col("solution_hash"),  # Include the UUID column for solution
        col("vendor_hash"),  # Include the UUID column for vendor
        *[col(column) for column in scanDetailColumns]
    )

    # Perform the join operation
    scandetail_df = final_df.join(
        host_df_with_uuid,
        on="scanDetail_hash",  # Specify the common column for the join
        how="left"  # Use left join to retain all rows from final_df
    )

    # Select all columns from final_df and only host_hash from host_df_with_uuid
    scandetail_df = scandetail_df.select(
        final_df.columns + [col("host_hash")]
    )



    if table_exists("tblScanDetail"):
        # Perform actions if the table exists
        print("The table exists. Proceeding with further operations.")

        # Get the latest data dates
        latest_data_date_from_data_in_repo = spark.table("tblscandetail") \
            .agg(max("dataDate").alias("latest_data_date")) \
            .first()["latest_data_date"]

        latest_data_date_coming_in = scandetail_df \
            .agg(max("dataDate").alias("latest_data_date_incoming")) \
            .first()["latest_data_date_incoming"]

        # Compare dates and update if necessary
        if latest_data_date_coming_in > latest_data_date_from_data_in_repo:
            print("Incoming dataDate is newer. Updating dataDateStatus with default values.")
            save_or_append_delta_table(scandetail_df, "tblScanDetail")

            update_dataDateStatusWithDefaultValues()

            data_date_status = scandetail_df \
                .filter(tbl_scan_detail_df["dataDate"] == latest_data_date_coming_in) \
                .select("dataDateStatus") \
                .first()["dataDateStatus"]
            
            print(f"Latest Data date : {data_date_status}")
            update_column_value_in_table("tblscandetail", "dataDateStatus", data_date_status, "Latest")
        else:
            print("Incoming dataDate is not newer. No updates made.")
    else:
        # Perform actions if the table does not exist
        print("The table does not exist. Creating the table with the specified schema.")

        # Add the dataDateStatus column to the DataFrame
        tbl_scan_detail_df = scandetail_df.withColumn("dataDateStatus", lit("Unknown").cast(StringType()))

        # Write the table to the Lakehouse partitioned by dataDate
        tbl_scan_detail_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("dataDate") \
            .saveAsTable("tblScanDetail")
        

        print("Table 'tblScanDetail' created successfully with partitioning by 'dataDate'.")


    
    
    # Unpersist cached DataFrames to free memory
    scandetaildf.unpersist()
    scandetaildf_with_uuid.unpersist()

    print(f"---------------------------------------------")
    print(f"VULN SCAN DETAIL IS COMPLETE")
    print(f"---------------------------------------------")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### setUpHostData

# CELL ********************

from pyspark.sql import functions as F


def setUpHostData(create_table = 1,pthToFiles = "Files/raw/qualys",tableName = "tblHost"):


    if create_table == 0:
        print(f"---------------------------------------------")
        print(f"GETTING HOST DETAIL FROM {pthToFiles}")
        print(f"---------------------------------------------")
    else: 
        print(f"---------------------------------------------")
        print(f"BEGINNING PROCESS OF SET UP OF HOST DETAIL")
        print(f"---------------------------------------------")

    print(f"Reading parquet files from {pthToFiles}")
    df = spark.read.parquet(pthToFiles)

    # Define the columns to select
    columns = ["DC_Host", "OSName", "NBName", "DNSName", "IPAddressStr", 
            "authStatus", "authenticationErrorDescription", "site_id", 
            "region", "location", "building", "managed", "HIPAA", 
            "PCI", "SOX", "ipInteger", "DMZ_Host", "deviceType", "network_id"]

    # Apply device type filter to select only 'Server' rows with non-null hostname
    device_type_filter = (col("deviceType") == "Server") & \
                        (coalesce(col("NBName"), col("DNSName")).isNotNull())
    filtered_df = df.filter(device_type_filter)

    # Group by the coalesced OSName and NBName, and create comma-delimited unique values for each column
    grouped_df = (
        filtered_df
        .groupBy(split(coalesce(col("NBName"), col("DNSName")), "\\.")[0].alias("hostname"))
        .agg(
            *[
            concat_ws(", ", sort_array(collect_set(col(col_name)))).alias(col_name)
            for col_name in columns
            ]
        )
    )

    # Count the rows in grouped_df
    row_count = grouped_df.count()
    print(f"Number of rows in the final dataset: {row_count}")
    grouped_df_with_uuid = add_uuid_column(grouped_df)


    print(f"Creating or Merging table '{tableName}' with selected columns and UUID.")
    create_table_with_id(grouped_df_with_uuid,tableName)
    print(f"Table '{tableName}' created successfully.")

    print(f"---------------------------------------------")
    print(f"HOST DETAIL PROCESSING COMPLETE")
    print(f"---------------------------------------------")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### setUpAtlasAppHardware

# CELL ********************

def setUpAtlasAppHardware(pthToFiles="Files/raw/odwAtlasAppHardware", tableName="tblAtlasAppHardware"):
    print(f"---------------------------------------------")
    print(f"BEGINNING PROCESS OF SET UP OF ATLAS APP HARDWARE DETAIL")
    print(f"---------------------------------------------")
    
    print(f"Reading parquet files from {pthToFiles}")
    df = spark.read.parquet(pthToFiles)
    
    # Define the columns to select
    columns = [
        "APP ID", "ENT ID", "Activity Based Costing Identifier", "Application Short Name", "Application Name",
        "ICT Object Name", "Application Version", "Application Status", "Application Support Code",
        "Application Owning Organization", "Application Owning Sub Organization", "Application Manager",
        "Server Name", "SOX Test Ready", "SOX Indicator", "Server Role", "Server Status",
        "Fully Qualified Domain Name", "Server Domain", "IP Address", "Server Serial Number", "Location",
        "Location Code", "Server Platform", "Operating System", "Operating System Build Number",
        "Deployment Type", "Deployment Comments", "Deployment Status", "RunSourceID", "FileDate",
        "IT Counterpart", "Server Primary Application POC", "Server Secondary Application POC",
        "Infrastructure Support", "Using Regions", "Server Subject to PCI Compliance",
        "Personally Identifiable Information", "Primary IP Address", "Component Version",
        "Component Name", "Component Flag", "Resiliency Tier", "Business Required RTO",
        "Business Required RPO", "Recovery Time Capability", "Recovery Point Capability",
        "DR Enabled", "Application Owning Org Level 2", "Application Owning Org Level 3",
        "Owning Org Level 1 Manager", "Owning Org Level 2 Manager", "Owning Org Level 3 Manager",
        "Server Subject to HIPAA Compliance", "Server Nature", "Application Owning Org Level 0",
        "Application Owning Org Level 1", "Product Name", "Server Subject to SOX Compliance",
        "Deployment ID", "Deployment Name", "Device Type", "MAC Address", "Primary MAC Address",
        "Member of Cluster", "Number of CPU", "CPU Speed", "CPU Type", "Asset Tag",
        "Source Unique Identifier", "Server Last Scan Date", "Server Description", "Primary ABC ID",
        "Regulatory/Customer Audits", "Server PII (Personally Identifiable Information)",
        "Application Owning Org Level 4", "Application Owning Org Level 5", "Application Owning Org Level 6",
        "Owning Org Level 1 Executive", "Owning Org Level 2 Executive", "Owning Org Level 3 Executive",
        "Owning Org Level 4 Executive", "Owning Org Level 5 Executive", "Owning Org Level 6 Executive"
    ]
    
    # Select the specified columns and rename to camelCase
    selected_df = df.select(*columns)
    for col_name in columns:
        new_col_name = to_camel_case(col_name)
        selected_df = selected_df.withColumnRenamed(col_name, new_col_name)

    # Remove duplicates
    selected_df = selected_df.distinct()
    
    # Count the rows in selected_df
    row_count = selected_df.count()
    print(f"Number of rows in the final dataset before insertion: {row_count}")

    # Create table with auto-incrementing ID
    print(f"Creating table '{tableName}' with selected columns and auto-incrementing ID.")
    create_table_with_id(selected_df, tableName)
    print(f"---------------------------------------------")
    print(f" ATLAS APP HARDWARE PROCESSING COMPLETE")
    print(f"---------------------------------------------")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Set Up Vuln Data

# CELL ********************

def setUpVulnData(pth_to_files="Files/raw/qualys", table_name="tblVuln"):
    """
    Processes vulnerability data by filtering, grouping, and setting up a Delta table.

    Parameters:
    - pth_to_files (str): Path to the raw parquet files.
    - table_name (str): Name of the Delta table to create or update.
    """
    print("---------------------------------------------")
    print("BEGINNING PROCESS OF SET UP OF VULNERABILITY DETAIL")
    print("---------------------------------------------")
    
    print(f"Reading parquet files from {pth_to_files}")
    df = spark.read.parquet(pth_to_files)
    
    # Define the columns to select
    columns = ["qid", "title", "category", "OS_App", "cvss_temporal", "cvss_v3_base", "cvss_v3_temporal"]
    
    # Apply a filter to select only 'Server' rows
    device_type_filter = col("deviceType") == "Server"
    filtered_df = df.filter(device_type_filter)
    
    # Group by `qid` and create pipe-delimited unique values for each column
    grouped_df = (
        filtered_df
        .groupBy(col("qid"))
        .agg(
            *[
                concat_ws(", ", sort_array(collect_set(col(col_name)))).alias(col_name)
                for col_name in columns if col_name != "qid"  # Exclude `qid` from aggregation
            ]
        )
    )
    
    # Count the rows in the final DataFrame
    row_count = grouped_df.count()
    print(f"Number of rows in the final dataset: {row_count}")

    # Create or merge the table
    print(f"Creating or merging table '{table_name}' with grouped data.")
    create_table_with_id(grouped_df, table_name)
    
    print("---------------------------------------------")
    print("VULNERABILITY DETAIL PROCESSING COMPLETE")
    print("---------------------------------------------")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get all Standard Tables

# CELL ********************

# Path to the Parquet file in the Lakehouse
def getAllStandardTables():
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
