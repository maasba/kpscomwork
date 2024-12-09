# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "874f0bb1-afc4-4cac-85aa-668087157a6a",
# META       "default_lakehouse_name": "lhSilver",
# META       "default_lakehouse_workspace_id": "ace43e1b-866a-4e00-a24b-b9997ff19815",
# META       "known_lakehouses": [
# META         {
# META           "id": "874f0bb1-afc4-4cac-85aa-668087157a6a"
# META         },
# META         {
# META           "id": "ff1ed9d9-4400-44ed-bf3f-255cec85cc0d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### saveAggAtlasdata

# CELL ********************


def saveAggAtlasData():
    # Load the table into a DataFrame
    df = spark.table("tblatlasapphardware")

    # Filter for the current records
    df = df.filter(col("isCurrent") == 1)

    # Select and trim relevant columns, ensuring deduplication within each column using distinct values
    trimmed_df = df.select(
        upper(trim(col("serverName"))).alias("Hostname"),
        trim(col("appId")).alias("appId"),
        trim(col("applicationShortName")).alias("applicationShortName"),
        trim(col("applicationName")).alias("applicationName"),
        trim(col("applicationSupportCode")).alias("applicationSupportCode"),
        trim(col("applicationOwningOrganization")).alias("applicationOwningOrganization"),
        trim(col("applicationOwningSubOrganization")).alias("applicationOwningSubOrganization"),
        trim(col("applicationManager")).alias("applicationManager"),
        trim(col("fullyQualifiedDomainName")).alias("fullyQualifiedDomainName"),
        trim(col("serverDomain")).alias("serverDomain"),
        trim(col("ipAddress")).alias("atlasIpAddress"),
        trim(col("serverSerialNumber")).alias("atlasServerSerialNumber"),
        trim(col("location")).alias("atlasLocation"),
        trim(col("locationCode")).alias("atlasLocationCode"),
        trim(col("serverPlatform")).alias("atlasServerPlatform"),
        trim(col("operatingSystem")).alias("atlasOperatingSystem"),
        trim(col("deploymentType")).alias("atlasEnvironment"),
        trim(col("filedate")).alias("atlasDataDate"),
        trim(col("itCounterpart")).alias("atlasItCounterpart"),
        trim(col("serverPrimaryApplicationPoc")).alias("atlasServerPrimaryApplicationPoc"),
        trim(col("serverSecondaryApplicationPoc")).alias("atlasServerSecondaryApplicationPoc"),
        trim(col("infrastructureSupport")).alias("atlasInfrastructureSupport"),
        trim(col("usingRegions")).alias("atlasUsingRegions"),
        trim(col("serverSubjectToPciCompliance")).alias("atlasServerSubjectToPciCompliance"),
        trim(col("personallyIdentifiableInformation")).alias("atlasPersonallyIdentifiableInformation"),
        trim(col("primaryIpAddress")).alias("atlasPrimaryIpAddress"),
        trim(col("drEnabled")).alias("atlasDrEnabled"),
        trim(col("applicationOwningOrgLevel2")).alias("atlasApplicationOwningOrgLevel2"),
        trim(col("applicationOwningOrgLevel3")).alias("atlasApplicationOwningOrgLevel3"),
        trim(col("owningOrgLevel1Manager")).alias("atlasOwningOrgLevel1Manager"),
        trim(col("owningOrgLevel2Manager")).alias("atlasOwningOrgLevel2Manager"),
        trim(col("owningOrgLevel3Manager")).alias("atlasOwningOrgLevel3Manager"),
        trim(col("serverSubjectToHipaaCompliance")).alias("atlasServerSubjectToHipaaCompliance"),
        trim(col("applicationOwningOrgLevel0")).alias("atlasApplicationOwningOrgLevel0"),
        trim(col("applicationOwningOrgLevel1")).alias("atlasApplicationOwningOrgLevel1"),
        trim(col("productName")).alias("atlasProductName"),
        trim(col("serverSubjectToSoxCompliance")).alias("atlasServerSubjectToSoxCompliance"),
        trim(col("deploymentId")).alias("atlasDeploymentId"),
        trim(col("deploymentName")).alias("atlasDeploymentName"),
        trim(col("deviceType")).alias("atlasDeviceType"),
        trim(col("macAddress")).alias("atlasMacAddress"),
        trim(col("primaryMacAddress")).alias("atlasPrimaryMacAddress"),
        trim(col("memberOfCluster")).alias("atlasMemberOfCluster"),
        trim(col("numberOfCpu")).alias("atlasNumberOfCpu"),
        trim(col("cpuSpeed")).alias("atlasCpuSpeed"),
        trim(col("cpuType")).alias("atlasCpuType"),
        trim(col("assetTag")).alias("atlasAssetTag"),
        trim(col("serverLastScanDate")).alias("atlasServerLastScanDate"),
        trim(col("primaryAbcId")).alias("atlasPrimaryAbcId"),
        trim(col("serverPiipersonallyIdentifiableInformation")).alias("atlasServerPiiPersonallyIdentifiableInformation"),
        trim(col("applicationOwningOrgLevel4")).alias("atlasApplicationOwningOrgLevel4"),
        trim(col("applicationOwningOrgLevel5")).alias("atlasApplicationOwningOrgLevel5"),
        trim(col("applicationOwningOrgLevel6")).alias("atlasApplicationOwningOrgLevel6"),
        trim(col("owningOrgLevel1Executive")).alias("atlasOwningOrgLevel1Executive"),
        trim(col("owningOrgLevel2Executive")).alias("atlasOwningOrgLevel2Executive"),
        trim(col("owningOrgLevel3Executive")).alias("atlasOwningOrgLevel3Executive"),
        trim(col("owningOrgLevel4Executive")).alias("atlasOwningOrgLevel4Executive"),
        trim(col("owningOrgLevel5Executive")).alias("atlasOwningOrgLevel5Executive"),
        trim(col("owningOrgLevel6Executive")).alias("atlasOwningOrgLevel6Executive")
    ).distinct()

    # Group by hostname and aggregate with deduplicated comma-separated lists for each column
    aggregated_df = trimmed_df.groupBy("Hostname").agg(
        concat_ws(" | ", collect_set("appId")).alias("atlasAppId"),
        concat_ws(" | ", collect_set("applicationShortName")).alias("atlasApplicationShortName"),
        concat_ws(" | ", collect_set("applicationName")).alias("atlasApplicationName"),
        concat_ws(" | ", collect_set("applicationSupportCode")).alias("atlasApplicationSupportCode"),
        concat_ws(" | ", collect_set("applicationOwningOrganization")).alias("atlasApplicationOwningOrganization"),
        concat_ws(" | ", collect_set("applicationOwningSubOrganization")).alias("atlasApplicationOwningSubOrganization"),
        concat_ws(" | ", collect_set("applicationManager")).alias("atlasApplicationManager"),
        concat_ws(" | ", collect_set("fullyQualifiedDomainName")).alias("atlasFullyQualifiedDomainName"),
        concat_ws(" | ", collect_set("serverDomain")).alias("atlasServerDomain"),
        concat_ws(" | ", collect_set("atlasIpAddress")).alias("atlasIpAddress"),
        concat_ws(" | ", collect_set("atlasServerSerialNumber")).alias("atlasServerSerialNumber"),
        concat_ws(" | ", collect_set("atlasLocation")).alias("atlasLocation"),
        concat_ws(" | ", collect_set("atlasLocationCode")).alias("atlasLocationCode"),
        concat_ws(" | ", collect_set("atlasServerPlatform")).alias("atlasServerPlatform"),
        concat_ws(" | ", collect_set("atlasOperatingSystem")).alias("atlasOperatingSystem"),
        concat_ws(" | ", collect_set("atlasEnvironment")).alias("atlasEnvironment"),
        concat_ws(" | ", collect_set("atlasDataDate")).alias("atlasDataDate"),
        concat_ws(" | ", collect_set("atlasItCounterpart")).alias("atlasItCounterpart"),
        concat_ws(" | ", collect_set("atlasServerPrimaryApplicationPoc")).alias("atlasServerPrimaryApplicationPoc"),
        concat_ws(" | ", collect_set("atlasServerSecondaryApplicationPoc")).alias("atlasServerSecondaryApplicationPoc"),
        concat_ws(" | ", collect_set("atlasInfrastructureSupport")).alias("atlasInfrastructureSupport"),
        concat_ws(" | ", collect_set("atlasUsingRegions")).alias("atlasUsingRegions"),
        concat_ws(" | ", collect_set("atlasServerSubjectToPciCompliance")).alias("atlasServerSubjectToPciCompliance"),
        concat_ws(" | ", collect_set("atlasPersonallyIdentifiableInformation")).alias("atlasPersonallyIdentifiableInformation"),
        concat_ws(" | ", collect_set("atlasPrimaryIpAddress")).alias("atlasPrimaryIpAddress"),
        concat_ws(" | ", collect_set("atlasDrEnabled")).alias("atlasDrEnabled"),
        concat_ws(" | ", collect_set("atlasApplicationOwningOrgLevel2")).alias("atlasApplicationOwningOrgLevel2"),
        concat_ws(" | ", collect_set("atlasApplicationOwningOrgLevel3")).alias("atlasApplicationOwningOrgLevel3"),
        concat_ws(" | ", collect_set("atlasOwningOrgLevel1Manager")).alias("atlasOwningOrgLevel1Manager"),
        concat_ws(" | ", collect_set("atlasOwningOrgLevel2Manager")).alias("atlasOwningOrgLevel2Manager"),
        concat_ws(" | ", collect_set("atlasOwningOrgLevel3Manager")).alias("atlasOwningOrgLevel3Manager"),
        concat_ws(" | ", collect_set("atlasServerSubjectToHipaaCompliance")).alias("atlasServerSubjectToHipaaCompliance"),
        concat_ws(" | ", collect_set("atlasApplicationOwningOrgLevel0")).alias("atlasApplicationOwningOrgLevel0"),
        concat_ws(" | ", collect_set("atlasApplicationOwningOrgLevel1")).alias("atlasApplicationOwningOrgLevel1"),
        concat_ws(" | ", collect_set("atlasProductName")).alias("atlasProductName"),
        concat_ws(" | ", collect_set("atlasServerSubjectToSoxCompliance")).alias("atlasServerSubjectToSoxCompliance"),
        concat_ws(" | ", collect_set("atlasDeploymentId")).alias("atlasDeploymentId"),
        concat_ws(" | ", collect_set("atlasDeploymentName")).alias("atlasDeploymentName"),
        concat_ws(" | ", collect_set("atlasDeviceType")).alias("atlasDeviceType"),
        concat_ws(" | ", collect_set("atlasMacAddress")).alias("atlasMacAddress"),
        concat_ws(" | ", collect_set("atlasPrimaryMacAddress")).alias("atlasPrimaryMacAddress"),
        concat_ws(" | ", collect_set("atlasMemberOfCluster")).alias("atlasMemberOfCluster"),
        concat_ws(" | ", collect_set("atlasNumberOfCpu")).alias("atlasNumberOfCpu"),
        concat_ws(" | ", collect_set("atlasCpuSpeed")).alias("atlasCpuSpeed"),
        concat_ws(" | ", collect_set("atlasCpuType")).alias("atlasCpuType"),
        concat_ws(" | ", collect_set("atlasassetTag")).alias("atlasassetTag"),
        concat_ws(" | ", collect_set("atlasserverLastScanDate")).alias("atlasserverLastScanDate"),
        concat_ws(" | ", collect_set("atlasprimaryAbcId")).alias("atlasprimaryAbcId"),
        concat_ws(" | ", collect_set("atlasserverPiipersonallyIdentifiableInformation")).alias("atlasServerPiiPersonallyIdentifiableInformation"),
        concat_ws(" | ", collect_set("atlasapplicationOwningOrgLevel4")).alias("atlasApplicationOwningOrgLevel4"),
        concat_ws(" | ", collect_set("atlasapplicationOwningOrgLevel5")).alias("atlasApplicationOwningOrgLevel5"),
        concat_ws(" | ", collect_set("atlasapplicationOwningOrgLevel6")).alias("atlasApplicationOwningOrgLevel6"),
        concat_ws(" | ", collect_set("atlasowningOrgLevel1Executive")).alias("atlasOwningOrgLevel1Executive"),
        concat_ws(" | ", collect_set("atlasowningOrgLevel2Executive")).alias("atlasOwningOrgLevel2Executive"),
        concat_ws(" | ", collect_set("atlasowningOrgLevel3Executive")).alias("atlasOwningOrgLevel3Executive"),
        concat_ws(" | ", collect_set("atlasowningOrgLevel4Executive")).alias("atlasOwningOrgLevel4Executive"),
        concat_ws(" | ", collect_set("atlasowningOrgLevel5Executive")).alias("atlasOwningOrgLevel5Executive"),
        concat_ws(" | ", collect_set("atlasowningOrgLevel6Executive")).alias("atlasOwningOrgLevel6Executive"))

    create_table_with_id(aggregated_df, "tblAtlasAggregatedByHost")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get The Scan Cves

# CELL ********************

def getScanCves():
    # Load the DataFrame from the parquet file
    pthToFiles = "Files/raw/qualys"
    df = spark.read.parquet(pthToFiles)

    # Filter for device type "Server"
    device_type_filter = col("deviceType") == "Server"
    filtered_df = df.filter(device_type_filter)

    # Select relevant columns
    main_df = filtered_df.select('OSName', 'CVE', 'qid')

    # Apply the case logic to categorize OS
    main_df = main_df.withColumn(
        "osCategory",
        when(lower(col("OSName")).like("%win%"), "Windows")
        .when(
            (lower(col("OSName")).like("%red hat%")) |
            (lower(col("OSName")).like("%rhel%")) |
            (lower(col("OSName")).like("%redhat%")), "RedHat"
        )
        .otherwise("Other")
    )

    # Split the comma-delimited 'CVE' column into an array and explode it into individual rows
    exploded_df = main_df.withColumn("CVE", explode(split(main_df["CVE"], ",")))

    # Remove any leading or trailing whitespace from each CVE entry
    exploded_df = exploded_df.withColumn("CVE", trim(exploded_df["CVE"]))

    # Select only the desired columns, exclude 'OSName', and apply distinct to remove duplicates
    final_df = exploded_df.select("qid", "osCategory", "CVE").distinct()
    
    # Return the final DataFrame
    return final_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get Big Fix Cve and Host data

# CELL ********************

def getBigFixFixlets():
    # Load the BigFix global fixlets table
    tblGlobalFixlets = spark.table("lhKpScom.tblbigfixglobalfixlets")

    # Select the relevant columns and rename "name" to "fixletName"
    bfGf_df = tblGlobalFixlets.select(
        col("datasource_fixlet_id").alias("fixletId"),
        col("name").alias("fixletName"),
        "cve"
    )

    # Apply the filter to exclude specific CVE values
    cve_filter = ~upper(col("cve")).isin("N/A", "UNSPECIFIED", "NA")
    bfGf_df = bfGf_df.filter(cve_filter)

    # Add the "osCategory" column based on the "fixletName" (previously "name") column
    bfGf_df = bfGf_df.withColumn(
        "osCategory",
        when(
            (lower(col("fixletName")).like("%win%")) | 
            (lower(col("fixletName")).like("%office 365%")), 
            "Windows"
        )
        .when(
            (lower(col("fixletName")).like("%red hat%")) |
            (lower(col("fixletName")).like("%rhel%")) |
            (lower(col("fixletName")).like("%redhat%")), "RedHat"
        )
        .otherwise("Other")
    )

    # Split the comma-delimited 'cve' column into an array and explode it into individual rows
    exploded_df = bfGf_df.withColumn("cve", explode(split(bfGf_df["cve"], ";")))

    # Remove any leading or trailing whitespace from each CVE entry
    exploded_df = exploded_df.withColumn("cve", trim(exploded_df["cve"]))

    # Return the distinct result
    return exploded_df.distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get QualysQid To Bigfix Fixlet info

# CELL ********************

def getQualysQidToBigFixFixlet():
    scancve_df = getScanCves()
    bfFixlet_df = getBigFixFixlets()

    # Perform the left join on the specified conditions
    joined_df = scancve_df.join(
        bfFixlet_df,
        (scancve_df["CVE"] == bfFixlet_df["cve"]) &
        (scancve_df["osCategory"] == bfFixlet_df["osCategory"]),
        "left"
    )
    # Select and rename columns to reflect their source
    joined_df = joined_df.select(
        "qid",
        scancve_df["osCategory"].alias("qualysOsCategory"),
        scancve_df["CVE"].alias("qualysCve"),
        "fixletId",                # From bfFixlet_df
        "fixletName",              # From bfFixlet_df
        bfFixlet_df["cve"].alias("bigFixCve"),
        bfFixlet_df["osCategory"].alias("bigFixosCategory")
    )

    # Perform the collect_set aggregation with custom delimiter and handle nulls with 'NOT DETECTED IN BIG FIX' for bfFixlet_df fields
    aggregated_df = joined_df.groupBy("qid").agg(
        F.concat_ws(" | ", F.collect_set("qualysOsCategory")).alias("qualysOsCategory"),
        F.concat_ws(" | ", F.collect_set("qualysCve")).alias("qualysCve"),
        
        # For fixletId
        F.when(
            F.concat_ws(" | ", F.collect_set("fixletId")).isNotNull(),
            F.concat_ws(" | ", F.collect_set("fixletId"))
        ).otherwise("NOT DETECTED IN BIG FIX").alias("fixletId"),
        
        # For fixletName
        F.when(
            F.concat_ws(" | ", F.collect_set("fixletName")).isNotNull(),
            F.concat_ws(" | ", F.collect_set("fixletName"))
        ).otherwise("NOT DETECTED IN BIG FIX").alias("fixletName"),
        
        # For bigFixCve
        F.when(
            F.concat_ws(" | ", F.collect_set("bigFixCve")).isNotNull(),
            F.concat_ws(" | ", F.collect_set("bigFixCve"))
        ).otherwise("NOT DETECTED IN BIG FIX").alias("bigFixCve"),
        
        # For bigFixosCategory
        F.when(
            F.concat_ws(" | ", F.collect_set("bigFixosCategory")).isNotNull(),
            F.concat_ws(" | ", F.collect_set("bigFixosCategory"))
        ).otherwise("NOT DETECTED IN BIG FIX").alias("bigFixosCategory")
    )

    # Show the result

    create_table_with_id(aggregated_df, "tblQualysToBigFix")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get Version Info From Results and Save to lake

# CELL ********************

from pyspark.sql.functions import regexp_replace, col, when, regexp_extract, expr

def getVersionInfoFromResultsAndSaveToLake():

        primary_pattern = r"(?i)\b(?:version|installed version|required version|lastclientversion|version is)\s*=?\s*(\d+(\.\d+)*[a-z]?)\b"
        secondary_pattern = r"(?i)\b(\d+(\.\d+)*(\-[\w\.]+)*)\b"

        # Load the table
        dfResults = spark.table("lhKpScom.tblvulnextresults")

        # Clean up the 'results' column
        dfResults = dfResults.withColumn(
            "cleaned_results",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(col("results"), r"[\r\n\t]", " "),  # Replace carriage returns, new lines, and tabs
                                r"\s{2,}", " "  # Reduce multiple spaces to one
                            ),
                            r"=+", " "  # Replace multiple '=' with a single space
                        ),
                        r"-{2,}", " "  # Replace multiple '-' with a single space
                    ),
                    r"&quot", " "  # Replace '&quot' with space
                ),
                r"&pos", " "  # Replace '&pos' with space
            )
        )

        # Replace multiple '#' symbols with a single space
        dfResults = dfResults.withColumn("cleaned_results", regexp_replace(col("cleaned_results"), r"#{2,}", " "))

        # Extract the software version using the patterns
        dfResults = dfResults.withColumn(
            "detectedSoftwareVersion",
            when(
                regexp_extract(col("results"), primary_pattern, 1) != "", 
                regexp_extract(col("results"), primary_pattern, 1)
            ).otherwise(
                when(
                    regexp_extract(col("results"), secondary_pattern, 1) != "", 
                    regexp_extract(col("results"), secondary_pattern, 1)
                ).otherwise("Software Version Not Found")
            )
        )

        # Limit 'results' column length for Power BI compatibility
        dfResults = dfResults.withColumn("results", expr("LEFT(cleaned_results, 32765)")).drop("cleaned_results")

        # Display the final DataFrame or save it to the Lakehouse
        saveDataFrameToLakeHouse(tableName='g_tblResults', dF=dfResults)
        #displayDataFrame(dfResults)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### getVuln Additional Data and Save to the lake house

# CELL ********************

from pyspark.sql.functions import col, collect_set, concat_ws

def get_vuln_add_data_and_save_to_lakehouse():
    vuln_df = spark.table("lhKpScom.tblvuln")

    df_kp_analysis = getKpAnalysisData()

    vuln_to_fixlet_df = getVulnToFixletData()

    df_jira = spark.table("tbljira")

    # Step 3: Group by Qid and apply collect_set, then concatenate values with '|'
    grouped_df_jira = df_jira.groupBy("Qid").agg(
        concat_ws("|", collect_set("jiraNumber")).alias("jiraNumbers"),
        concat_ws("|", collect_set("createDate")).alias("createDates"),
        concat_ws("|", collect_set("lastUpdated")).alias("lastUpdatedDates"),
        concat_ws("|", collect_set("jiraType")).alias("jiraTypes"),
        concat_ws("|", collect_set("treatmentstatus")).alias("treatmentStatuses"),
        concat_ws("|", collect_set("treatmentReleaseVersion")).alias("treatmentReleaseVersions"),
        concat_ws("|", collect_set("treatmentSummary")).alias("treatmentSummaries"),
        concat_ws("|", collect_set("QidDescription")).alias("qidDescriptions")
    )

    grouped_df_jira = grouped_df_jira.withColumnRenamed("Qid", "jiraQid")

    # Join vuln_df with vuln_to_fixlet_df and df_kp_analysis
    result_df = (
        vuln_df
        .join(vuln_to_fixlet_df, vuln_df["qid"] == vuln_to_fixlet_df["fixlet_qid"], how="left")
        .join(df_kp_analysis, vuln_df["qid"] == df_kp_analysis["kpAnalysis_qid"], how="left")
        .join(grouped_df_jira, vuln_df["qid"] == grouped_df_jira["jiraQid"], how="left")
    )

    # Select relevant fields, ensuring there is no ambiguity with `qid`
    result_df_selected = result_df.select(
            *[col(column) for column in vuln_df.columns],
            col("qualysOsCategory"),
            col("qualysCve"),
            col("fixletId"),
            col("fixletName"),
            col("bigFixCve"),
            col("bigFixosCategory"),
            *[col(column) for column in df_kp_analysis.columns if column not in ["kpAnalysis_qid","kpAnalysis_Count_of_hostID"]],
            *[col(column) for column in grouped_df_jira.columns if column not in ["jiraQid"]]

    )




    # Display or save the final DataFrame
    #displayDataFrame(result_df_selected)
    # Optionally, save to the lakehouse
    saveDataFrameToLakeHouse(tableName='g_tblVuln', dF=result_df_selected)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get vuln to fixlet data

# CELL ********************

def getVulnToFixletData():
    # Load and filter vulnToFixlet DataFrame and rename `qid` to avoid ambiguity
    vuln_to_fixlet_df = spark.table("lhKpScom.tblqualystobigfix") \
        .filter(col("iscurrent") == 1) \
        .withColumnRenamed("qid", "fixlet_qid") \
        .select(
            col("fixlet_qid"),
            col("qualysOsCategory"),
            col("qualysCve"),
            col("fixletId"),
            col("fixletName"),
            col("bigFixCve"),
            col("bigFixosCategory")
        )
    return vuln_to_fixlet_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get kp analysis data

# CELL ********************

def getKpAnalysisData():
    df_kp_analysis=spark.table("tblkpVulnDispostion")

    # Group by `QID` and aggregate other fields by concatenating unique values with the prefix 'kpAnalysis'
    df_kp_analysis = (
        df_kp_analysis
        .groupBy("qid")  # Group by QID
        .agg(
            *[
                concat_ws(" | ", collect_set(col(column))).alias(f"kpAnalysis_{column}")
                for column in df_kp_analysis.columns if column != "qid"
            ]
        )
        .withColumnRenamed("qid", "kpAnalysis_qid")  # Rename QID to avoid ambiguity
    )

    df_kp_analysis.drop("kpAnalysis_Count_of_hostID")
    
    return df_kp_analysis

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get Version info from the solutions Table

# CELL ********************

from pyspark.sql.functions import col, regexp_extract, lower, when, expr

def getVersionInfoFromSolutionsAndSaveToLake():
    # Define patterns with case-insensitivity
    primary_pattern = r"(?i)\b(?:version|installed version|required version|lastclientversion|version is)\s*=?\s*(\d+(\.\d+)*[a-z]?)\b"
    secondary_pattern = r"(?i)\b(\d+(\.\d+)*(\-[\w\.]+)*)\b"
    url_pattern_html = r"(?i)(https?://[^\s\"'>]+|ftp://[^\s\"'>]+)"  # Updated for HTML tags and special characters

    # Filter, extract, and add default values if no match is found
    dfResults = (
        spark.table("lhKpScom.tblvulnextsolution")
        
        # Extract version information in a case-insensitive manner
        .withColumn(
            "kpAnalysisSoftwareVersion",
            when(
                regexp_extract(lower(col("solution")), primary_pattern, 1) != "",
                regexp_extract(lower(col("solution")), primary_pattern, 1)
            ).otherwise(
                when(
                    regexp_extract(lower(col("solution")), secondary_pattern, 1) != "",
                    regexp_extract(lower(col("solution")), secondary_pattern, 1)
                ).otherwise("Software Version Not Found")
            )
        )
        
        # Extract the last URL found in the solution field (case-insensitive)
        .withColumn(
            "kpAnalysisURL",
            when(
                regexp_extract(lower(col("solution")), url_pattern_html, 0) != "",
                regexp_extract(lower(col("solution")), url_pattern_html, 0)
            ).otherwise("No URL Found")
        )
        
        # Determine the category (patch/upgrade) based on keywords in the solution field (case-insensitive)
        .withColumn(
            "kpAnalysisCategory",
            when(lower(col("solution")).contains("patch"), "patch")
            .when(lower(col("solution")).contains("upgrade"), "upgrade")
            .when(lower(col("solution")).contains("update"), "upgrade")
            .otherwise("No Category Found")
        )
        
        # Select and rename columns
        .select("uuid_hash", "kpAnalysisSoftwareVersion", "kpAnalysisURL", "kpAnalysisCategory", "solution")
    )

    # Optionally save to lake house
    saveDataFrameToLakeHouse(tableName='g_tblSolution', dF=dfResults)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Set up Gold tblVuln table

# CELL ********************

def getGoldTblHost():
    # Ensure dfManagedBy and dfAtlas are defined
    dfManagedBy = getManagedBy()
    dfAtlas = getAtlasAggregatedDetail()
    dfTblHost = spark.table("lhKpScom.tblhost")

    # Perform the left joins and select columns with adjustments
    dfJoined = (
        dfTblHost
        .join(dfManagedBy, lower(dfTblHost["hostname"]) == lower(dfManagedBy["hostname"]), how="left")
        .join(dfAtlas, lower(dfTblHost["hostname"]) == lower(dfAtlas["hostname"]), how="left")
        .select(
            upper(dfTblHost["hostname"]).alias("hostname"),  # Uppercase hostname from dfTblHost
            dfManagedBy["managedBy"],
            *[col(column) for column in dfTblHost.columns if column != "hostname"],  # Other columns from dfTblHost
            *[col(column) for column in dfAtlas.columns if column not in ["hostname", "Hostname"]]  # Columns from dfAtlas, excluding specified columns
        )
    )

    # Save the joined DataFrame to the lake house
    saveDataFrameToLakeHouse(tableName='g_tblhost', dF=dfJoined)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getAtlasAggregatedDetail():
    # Load the table and rename the "Hostname" column to "hostname"
    dfAtlas = (
        spark.table("lhKpScom.tblatlasaggregatedbyhost")
        .withColumnRenamed("Hostname", "hostname")
        .select(
            *[col(column) for column in spark.table("lhKpScom.tblatlasaggregatedbyhost").columns if column not in ["effectiveDate", "Hostname", "endDate", "isCurrent", "lastUpdated","uuid_hash"]]
        ).filter(col("isCurrent") == 1)
    )
    return dfAtlas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getManagedBy():
    dfManagedBy = (
    spark.table("lhKpScom.tbluhc")
    .withColumn(
                "managedby",
                when(col("is_managed_by") == 1, "1 - BIGFIX AUTO PATCH & AUTO REBOOT")
                .when(col("is_managed_by") == 13, "13 - CLOUD LINUX - BIGFIX AUTO PATCH/BP MANUAL REBOOT")
                .when(col("is_managed_by") == 14, "14 - UNIX PATCH TESTING - PHASE 1 (PHYSICAL SERVERS ONLY)")
                .when(col("is_managed_by") == 19, "19 - VENDOR MANAGED")
                .when(col("is_managed_by") == 2, "2 - BIGFIX AUTO PATCH & IBM MANUAL REBOOT")
                .when(col("is_managed_by") == 20, "20 - UNIX AUTO-PATCH TESTING - PHASE 2 (PHYSICAL SERVERS ONLY)")
                .when(col("is_managed_by") == 21, "21 - UNIX MANUAL PATCHING/REBOOTING")
                .when(col("is_managed_by") == 3, "3 - BIGFIX AUTO PATCH & BUSINESS PARTNER/VENDOR MANUAL REBOOT")
                .when(col("is_managed_by") == 5, "5 - BIGFIX AUTO PATCH & DELAYED AUTO REBOOT")
                .when(col("is_managed_by") == 7, "7 - BIOMED MANAGED")
                .when(col("is_managed_by") == 8, "8 - SELF MANAGED")
                .when(col("is_managed_by") == 99, "99-ABANDONED SERVERS (UNKNOWN OWNERS) - QUARANTINE ELIGIBLE")
                .otherwise("UNKNOWN")  # Default value for unrecognized or null entries
            )
    .select ("hostname","managedBy")
    .groupBy("hostname")
    .agg(spark_max("managedBy").alias("managedBy"))
    )
    return dfManagedBy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
