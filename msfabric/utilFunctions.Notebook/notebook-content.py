# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ### Imports

# CELL ********************

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, udf, monotonically_increasing_id, lit, coalesce, when, trim, 
    collect_set, concat_ws, split, collect_list, max as spark_max, upper, lower,date_format, sort_array, explode
)
from pyspark.sql.types import IntegerType, TimestampType, BooleanType, LongType, StringType
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from functools import reduce
from typing import List, Optional, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import zlib
import uuid
import pandas as pd
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 def explode_data_column(src_table_name: str, hash_column: str, column_to_explode: str, new_table_name: str):
    """
    Explodes a pipe-delimited column into multiple rows, keeping the hash column intact.
    The results are displayed using `displayDataFrame`.
    This only works against the g_tblVuln table and is used to create lookup tables for slicers in 
    power bi

    Args:
        src_table_name (str): The name of the table that this function should check for the columns to explode on.
        hash_column (str): The name of the hash column to retain in the exploded DataFrame.
        column_to_explode (str): The name of the column to split and explode.
        new_table_name (str): The name of the resulting table for future use (if saving is needed).
    """
    # Step 1: Define the source table
    main_table = src_table_name  # The table containing vulnerability data

    # Generate dynamic column names
    array_column_name = column_to_explode + "Array"

    # Load the source table into a DataFrame
    df = spark.table(main_table).select(hash_column, column_to_explode)

    # Step 2: Split the pipe-delimited column into an array
    # This creates a new array column from the delimited values
    df_split = df.withColumn(array_column_name, split(df[column_to_explode], "\\|"))

    # Step 3: Explode the array column into multiple rows
    # Each value in the array becomes a separate row, duplicating the hash column
    df_exploded = df_split.select(
        hash_column,
        explode(df_split[array_column_name]).alias(column_to_explode)  # Exploded values retain the original column name
    )

    # Step 4: Display the resulting DataFrame (for visualization purposes)
    saveDataFrameToLakeHouse(tableName=new_table_name, dF=df_exploded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def ensure_column_exists(table_name, column_name, column_type=StringType(), default_value=""):
    """
    Ensures that the specified column exists in the Delta table.
    If it does not exist, adds the column with the specified default value.
    
    Parameters:
    - table_name (str): The name of the Delta table.
    - column_name (str): The name of the column to ensure exists.
    - column_type (DataType): The type of the column to ensure.
    - default_value: The default value for the column if it needs to be added.
    """
    try:
        # Check if the table exists
        if DeltaTable.isDeltaTable(spark, table_name):
            print(f"Table '{table_name}' found.")
            df = spark.table(table_name)
            
            # Check if the column exists and is of the correct type
            schema = dict(df.dtypes)
            if column_name not in schema:
                print(f"Column '{column_name}' does not exist. Adding it with default value '{default_value}'.")
                df = df.withColumn(column_name, lit(default_value).cast(column_type))
            elif schema[column_name] != column_type.typeName():
                print(f"Column '{column_name}' exists but is not of type '{column_type.typeName()}'. Correcting the type.")
                df = df.withColumn(column_name, col(column_name).cast(column_type))
            else:
                print(f"Column '{column_name}' already exists and is of the correct type.")
                return

            # Overwrite the table with the modified DataFrame
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            print(f"Column '{column_name}' has been added or corrected in table '{table_name}'.")
        else:
            print(f"Table '{table_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred while processing the table '{table_name}': {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_dataDateStatusWithDefaultValues(table_name = "tblscandetail"):
    """
    Updates the `dataDateStatus` column in the specified Delta table, 
    setting it to the string representation of `dataDate` only if `dataDateStatus` is null or "Current".

    Parameters:
    - table_name (str): The name of the Delta table to update.
    """
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # Define the Delta table
    delta_table = DeltaTable.forName(spark, table_name)

    # Update `dataDateStatus` only if it is null or "Current"
    delta_table.update(
        condition=(
            (col("dataDateStatus").isNull()) | 
            (col("dataDateStatus").isin("Current", "NotCurrent", "Unknown")) & 
            col("dataDate").isNotNull()
        ),
        set={"dataDateStatus": date_format(col("dataDate"), "yyyy-MM-dd HH:mm:ss")}  # Format as string
    )

    print(f"Updated '{table_name}' by setting 'dataDateStatus' to the string format of 'dataDate' where 'dataDateStatus' is null or 'Current'.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### update a column on a delta table

# CELL ********************

def update_column_value_in_table(table_name, column_name, old_value, new_value):
    """
    Updates a specified column in a Delta table, setting rows with `old_value` to `new_value`.

    Parameters:
    - table_name (str): The name of the Delta table to update.
    - column_name (str): The column name to check for `old_value`.
    - old_value (str): The value to be replaced.
    - new_value (str): The new value to set for rows where `column_name` matches `old_value`.
    """
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # Define the Delta table
    delta_table = DeltaTable.forName(spark, table_name)

    # Execute the update
    delta_table.update(
        condition=col(column_name) == lit(old_value),  # Wrap old_value in lit() to treat it as a literal
        set={column_name: lit(new_value)}  # Wrap new_value in lit() to treat it as a literal
    )

    print(f"Rows in '{table_name}' where '{column_name}' = '{old_value}' have been updated to '{new_value}'.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_qualys_src_latest_data_date(pth_to_files="Files/raw/qualys"):
    """
    Retrieves the latest `processedDateTime` from the Qualys source data.

    Parameters:
    - pth_to_files (str): Path to the Parquet files containing Qualys data.

    Returns:
    - datetime: The latest processedDateTime value from the data.
    """
    try:
        # Read the Parquet file into a DataFrame
        df = spark.read.parquet(pth_to_files)
        
        # Extract the first `processedDateTime` value
        processed_date_time = df.select("processedDateTime").first()[0]
        
        return processed_date_time
    except Exception as e:
        print(f"Error retrieving `processedDateTime`: {e}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Ok to process scan Data

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import max

def OkToProcessScanData(pth_to_files="Files/raw/qualys", table_name="tblScanDetail"):
    """
    Checks if it is OK to process scan data based on processedDateTime and dataDate.

    Parameters:
    - pth_to_files (str): Path to the Parquet file to read.
    - table_name (str): Name of the Delta table to check.

    Returns:
    - bool: True if conditions are met to process scan data, False otherwise.
    """
 
    # Read the Parquet file into a DataFrame
    print(f"Reading parquet files from {pth_to_files}")
    df = spark.read.parquet(pth_to_files)
    srcQualysLatestdataDate=get_qualys_src_latest_data_date()

    # Load the existing table from the Lakehouse
    try:
        tbl_scan_detail_df = spark.table(table_name)
    except Exception as e:
        print(f"Error loading table '{table_name}': {e}")
        return False  # Exit function if table loading fails

    # Check if 'dataDate' column exists in tblScanDetail
    if "dataDate" in tbl_scan_detail_df.columns:
        # Get the maximum 'dataDate' value
        latest_data_date = tbl_scan_detail_df.agg(max("dataDate").alias("latest_data_date")).first()["latest_data_date"]
        print("Latest data date:", latest_data_date)
    else:
        print("Column 'dataDate' not found in 'tblScanDetail'")
        return False  # Exit function if column is missing

    # Determine if it is OK to process the scan data
    if srcQualysLatestdataDate > latest_data_date:
        print("Incoming Data is newer met. OK to process scan data.")
        return True
    else:
        print("Incoming data is not newer than existing data. Not OK to process scan data.")
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### add column to delta table.

# CELL ********************

def add_column_to_delta_table(table_name: str, column_name: str, default_value):
    """
    Adds a new column with a specified default value to an existing Delta table.

    Parameters:
    - table_name (str): The name of the Delta table to update.
    - column_name (str): The name of the new column to add.
    - default_value: The default value to set for the new column.

    add_column_to_delta_table("tblscandetail", "dataDateStatus", "Current")
    """
  

    # Load the existing Delta table as a DataFrame
    df = spark.table(table_name)

    # Add the new column with the specified default value
    df_with_new_column = df.withColumn(column_name, lit(default_value))

    # Write the DataFrame back to the Delta table with schema evolution enabled
    df_with_new_column.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)

    print(f"Column '{column_name}' added to table '{table_name}' with default value '{default_value}'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Define the uuid Function

# CELL ********************

def generate_uuid(*fields):
    """
    Generates a UUID for fields in a Spark DataFrame row by concatenating all fields.
    Parameters:
        *fields: Variable arguments for each column in the row
    Returns:
        str: A UUID string.
    """
    print(f"Generating UUID for: {fields}")
    # Concatenate all field strings with a separator
    combined_fields = '|'.join([str(field) if field is not None else 'null' for field in fields])
    # Generate a UUID based on the combined fields
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, combined_fields))

print("Registering the UUID UDF")
# Register the UDF
uuid_udf = udf(generate_uuid, StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### add uuid column to a dataframe

# CELL ********************

# Define the function to add CRC32 column
def add_uuid_column(df, exclude_columns=None):
    """
    Adds a CRC32 column to the DataFrame, excluding specified columns.
    Excludes columns that start with 'sk' by default.
    
    Parameters:
    df (pyspark.sql.DataFrame): The DataFrame to add the uuid column to.
    exclude_columns (list): Additional columns to exclude from CRC calculation.
    
    Returns:
    pyspark.sql.DataFrame: DataFrame with the uuid column added.
    """
    print("Attempting to add the UUID column to the dataframe")

    # Check if the UUID column already exists
    if "uuid_hash" in df.columns:
        print("The 'uuid_hash' column already exists. Returning the original DataFrame.")
        return df

    # Default columns to exclude
    default_exclusions = ['effectiveDate', 'endDate', 'isCurrent', 'lastUpdated']
    
    # Combine user-specified exclusions with defaults
    if exclude_columns:
        print(f"These columns excluded from uuid generation: {exclude_columns}")
        exclude_columns = set(default_exclusions + exclude_columns)
    else:
        print(f"These columns excluded from uuid generation : {default_exclusions}")
        exclude_columns = set(default_exclusions)
    
    # Filter columns to include in the CRC calculation
    included_columns = [
        col_name for col_name in df.columns 
        if col_name not in exclude_columns and not re.match(r'^sk', col_name)
    ]
    print(f"These columns INCLUDED in UUID generation : {included_columns}")
    # Apply the UDF to calculate the CRC32 hash based on the included columns
    df_with_uuid = df.withColumn("uuid_hash", uuid_udf(*[col(c) for c in included_columns]))
    print("UUID column has been evaluated")
    
    return df_with_uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### delete null rows

# CELL ********************

def delete_null_rows(table_name: str, field_name: str):
    """
    Deletes rows with null values in the specified field from a Delta table.

    Parameters:
    - table_name (str): The name of the Delta table.
    - field_name (str): The name of the column to check for null values.
    """
    try:
        # Load the Delta table
        delta_table = DeltaTable.forName(spark, table_name)
        
        # Perform the delete operation where the specified field is null
        condition = f"{field_name} IS NULL"
        delta_table.delete(condition)

        print(f"Rows with null values in '{field_name}' have been deleted from '{table_name}'.")

    except Exception as e:
        print(f"An error occurred while deleting null rows from '{table_name}': {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### convert to camel case

# CELL ********************

# Function to convert to camel case and remove spaces
def to_camel_case(column_name):
    parts = column_name.strip().split(" ")
    camel_case_name = parts[0].lower() + ''.join(word.capitalize() for word in parts[1:])
    sanitized_name = re.sub(r'[^a-zA-Z0-9]', '', camel_case_name)  # Remove invalid characters
    return sanitized_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get vulnerability source subsets

# CELL ********************

def getVulnSrcSubset(columns_to_select, tableName, pthToFiles):
    """
    Reads a parquet file, selects specified columns, filters by device type and column values,
    and creates a table with an auto-incrementing ID.
    
    Parameters:
    columns_to_select (list): List of column names to include in the final dataset.
    tableName (str): Name of the target table to create.
    pthToFiles (str): Path to the parquet files.
    """
    # Read the parquet file
    print(f"---------------------------------------------")
    print(f"BEGINNING PROCESS OF SET UP OF {tableName}")
    print(f"---------------------------------------------")
    print(f"Reading parquet files from {pthToFiles}")
    df = spark.read.parquet(pthToFiles)
    
    # Define the base filter condition for `deviceType`
    print(f"setting the filter so that it includes rows that have a device type as server for : {pthToFiles}")
    device_type_filter = col("deviceType") == "Server"
    
    # Apply additional filtering based on the number of columns passed
    if len(columns_to_select) == 1:
        # Filter for device type and exclude NULL values in the single column
        print(f"setting additional filters to ensure that null values are filtered out for : {col(columns_to_select[0])}")
        filtered_df = df.filter(device_type_filter & col(columns_to_select[0]).isNotNull())
    else:
        # If more than one column is passed, filter only by device type
        print(f"Confirming that there is only 1 column so the default filter of device type = server will be the only filter")
        filtered_df = df.filter(device_type_filter)
    
    # Select specified columns after filtering and ensure distinct rows
    print(f"Setting it so we only get distinct values")
    selected_df = filtered_df.select(*columns_to_select).distinct()
    
    row_count = selected_df.count()
    print(f"Number of rows in the final dataset before insertion: {row_count}")
    
    # Create table with auto-incrementing ID
    print(f"Creating table '{tableName}' with selected columns and uuid.")
    create_table_with_id(selected_df, tableName)
    print(f"---------------------------------------------")
    print(f" {tableName} PROCESSING IS COMPLETE")
    print(f"---------------------------------------------")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### get dataframe schema

# CELL ********************

def getDataFrameSchema(pathToSrcFiles):
    print(f"Grabbing the files from '{pathToSrcFiles}'.")

    print("Creating the Spark DataFrame that points to the file")
    # Here you would select specific columns that you want; otherwise, the entire table will go through.
    df = spark.read.parquet(pathToSrcFiles)

    print("Source DataFrame Columns:")
    print("\n".join(f"  - {col}" for col in df.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def processSrcfilesAllFields(pathToSrcFiles,tblName):
    print(f"Grabbing the files from '{pathToSrcFiles}' ." )
 

    print("creating the spark frame that points to the file")
    #here you would select the specific columns that you want otherwise let the entire table go through
    df = spark.read.parquet(pathToSrcFiles)

    create_table_with_auto_id(df,tblName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save or Append data

# CELL ********************

def save_or_append_delta_table(df, table_name):
    """
    Saves or appends to a Delta table with schema evolution.

    Parameters:
    - df (DataFrame): DataFrame to save or append.
    - table_name (str): Name of the Delta table in the catalog.
    """
    # Count records in the incoming DataFrame
    record_count = df.count()
    print(f"Number of records in DataFrame: {record_count}")
    
    if spark.catalog.tableExists(table_name):
        # Table exists, append with schema evolution
        print(f"Table '{table_name}' exists. Appending {record_count} records with schema evolution...")
        
        # Append records with schema evolution
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        print(f"Successfully appended {record_count} records to '{table_name}' with schema evolution.")
    
    else:
        # Table does not exist, create it
        print(f"Table '{table_name}' does not exist. Creating table with {record_count} records...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Successfully created '{table_name}' with {record_count} records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_exists(table_name: str) -> bool:
    """
    Checks if a table exists in the current Spark session.

    Parameters:
    - table_name (str): The name of the table to check.

    Returns:
    - bool: True if the table exists, False otherwise.
    """
    try:
        # Use Spark catalog to check for the table
        return spark.catalog.tableExists(table_name)
    except Exception as e:
        # Handle unexpected errors gracefully
        print(f"Error checking table existence: {e}")
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get File metadata

# CELL ********************

def get_file_metadata(filepth):
    
    path = filepth

    # Access the Hadoop file system through SparkContext
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file_statuses = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))

    # Extract file paths and modification dates
    file_info = []
    for file_status in file_statuses:
        file_path = file_status.getPath().toString()
        modification_time = file_status.getModificationTime()  # Get time in milliseconds since epoch
        # Convert to a human-readable datetime
        modification_datetime = datetime.fromtimestamp(modification_time / 1000)
        file_info.append((file_path, modification_datetime))

    # Display file paths and modification dates
    for file_path, mod_date in file_info:
        print(f"File: {file_path}")
        print(f"Last Modified: {mod_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Merge With The Lakehouse

# CELL ********************

def merge_with_lakehouse(source_df, target_table_name,joinkey = "uuid_hash", dateFromFile=None):
    """
    Merges source DataFrame with a target Delta table based on `uuid_hash`, handling new, updated, and inactive records.
    
    Parameters:
    - source_df (pyspark.sql.DataFrame): Source DataFrame to merge with Delta table.
    - target_table_name (str): Name of the target Delta table.
    """
    print("Starting Slowly Changing dimension evaluations operation.")
 

    # Ensure the function `get_qualys_src_latest_data_date` is defined
    try:
        latest_qualys_data_date = get_qualys_src_latest_data_date()  # Fetch the latest data date
        #right now everything is going to load with the qualys date. need to adjust that in the future to get the date from the source
    except NameError:
        print("Error: 'get_qualys_src_latest_data_date' is not defined.")
        return

    current_datetime = datetime.now()
    source_df.show(10)

    try:
        # Load target table and check for schema compatibility
        target_table = DeltaTable.forName(spark, target_table_name)
        target_df = target_table.toDF()
        
        print("target table found and turned to a dataframe.")

        if dict(source_df.dtypes).get(joinkey) != dict(target_df.dtypes).get(joinkey):
                source_dtype = dict(source_df.dtypes).get(joinkey)
                target_dtype = dict(target_df.dtypes).get(joinkey)
                print(f"Data type mismatch on {joinkey}.")
                print(f"Source Data Type: {source_dtype}")
                print(f"Target Data Type: {target_dtype}")
                print(f"Data type mismatch on {joinkey}. SCD Evaluation aborted.")
                return

        print(f"{joinkey} found in both tables and the data types match.")

        initial_target_count = target_df.count()
        source_count = source_df.count()
        matched_count = target_df.join(source_df, joinkey).count()
        



        # Perform the Delta merge
        target_table.alias("tgt").merge(
            source_df.alias("src"),
            f"tgt.{joinkey} = src.{joinkey}"  # Use joinkey variable here
        ).whenMatchedUpdate(condition="tgt.isCurrent = False", set={
            "isCurrent": lit(True),
            "endDate": lit(None),
            "lastUpdated": lit(current_datetime)
        }).whenNotMatchedBySourceUpdate(set={
            "isCurrent": lit(False),
            "endDate": lit(current_datetime),
            "lastUpdated": lit(current_datetime)
        }).whenNotMatchedInsert(values={
            joinkey: col(f"src.{joinkey}"),  # Use joinkey variable here
            "effectiveDate": lit(latest_qualys_data_date),
            "isCurrent": lit(True),
            "lastUpdated": lit(current_datetime),
            **{col_name: col(f"src.{col_name}") for col_name in source_df.columns if col_name != joinkey}
        }).execute()

        # Post-merge counts
        final_target_count = target_table.toDF().count()
        new_record_count = final_target_count - initial_target_count + matched_count - source_count
        deleted_record_count = initial_target_count - matched_count

        print(f"Matched records: {matched_count}")
        print(f"New records: {new_record_count}")
        print(f"Inactive records (not in source): {deleted_record_count}")

        # Optimize table after merge
        spark.sql(f"OPTIMIZE {target_table_name}")
        print(f"Table '{target_table_name}' optimized.")

    except Exception as e:
        if "not a Delta table" in str(e) or "not found" in str(e).lower():
            print(f"Table '{target_table_name}' not found. Creating new Delta table.")
            source_df.withColumn("effectiveDate", lit(latestQualys_dataDate)) \
                .withColumn("isCurrent", lit(True)) \
                .withColumn("lastUpdated", lit(current_datetime)) \
                .write.format("delta").mode("overwrite").saveAsTable(target_table_name)
            print(f"Table '{target_table_name}' created.")
        else:
            print(f"Error: {e}. No updates applied to '{target_table_name}'.")
        
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add Scd Columns

# CELL ********************

def add_scd2_columns(df):
    """
    Adds Slowly Changing Dimension (SCD) Type 2 columns to a DataFrame.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to add columns to.
    
    Returns:
    pd.DataFrame: Updated DataFrame with SCD2 columns added if they don't already exist.
    """
    # Current datetime for `lastUpdated` column
    print(f"Checking on the data frame to ensure that the SCD fields are present.")
    current_datetime = datetime.now()
    qualysLatestDataDate = get_qualys_src_latest_data_date()

    # Check if 'effectiveDate' column exists, if not, add it
    if 'effectiveDate' not in df.columns:
        df = df.withColumn('effectiveDate', lit(qualysLatestDataDate).cast(TimestampType()))
        print(f"Effective date is not in this dataFrame. Added the effective date to the dataframe.")

    # Check if 'endDate' column exists, if not, add it
    if 'endDate' not in df.columns:
        df = df.withColumn('endDate', lit(None).cast(TimestampType()))
        print(f"End date is not in this dataFrame. Added the End date to the dataframe.")

    # Check if 'isCurrent' column exists, if not, add it
    if 'isCurrent' not in df.columns:
         df = df.withColumn('isCurrent', lit(True).cast(BooleanType()))
         print(f"IsCurrent Flag is not in this dataFrame. Added the IsCurrent Flag to the dataframe.")

    # Check if 'lastUpdated' column exists, if not, add it
    if 'lastUpdated' not in df.columns:
        df = df.withColumn('lastUpdated', lit(current_datetime).cast(TimestampType()))
        print(f"lastUpdated date is not in this dataFrame. Added the lastUpdated date to the dataframe.")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### create_table_with_id

# CELL ********************

def create_table_with_id(df, table_name, dateFromFile=None):
    """
    Checks if a table exists in the Lakehouse. If it does not exist, 
    creates the table with an automatically uuid column.

    Parameters:
    df (pyspark.sql.DataFrame): The DataFrame containing the data.
    table_name (str): The name of the table to check/create.
    """
   # Add SCD2 columns if they don't already exist
    print(f"Checking of the SCD columns exist on the data frame")
    df = add_scd2_columns(df)
    
    df_with_id = add_uuid_column(df)
    print(f"uuid column has been added to the dataframe")
    df_with_id.show(10)

    #Check for any variation of 'Hostname' and standardize it
    hostname_columns = [col_name for col_name in df.columns if col_name.lower() == "hostname"]
    if hostname_columns:
        print(f"Renaming column '{hostname_columns[0]}' to 'hostname' and converting values to uppercase.")
        df_with_id = df_with_id.withColumnRenamed(hostname_columns[0], "hostname").withColumn("hostname", upper(col("hostname")))



    # Check if the table already exists
    try:
        # Try to load the table

        print(f"Checking if  '{table_name}' Exists ")
        spark.table(table_name)
        print(f"the '{table_name}' Exists So attempting to do a merge")
        merge_with_lakehouse(df_with_id,table_name)
        print(f"Table '{table_name}' Exists So did a merge with the new data set")
    except AnalysisException:
        # Table does not exist, so we proceed to create it
        print(f"Table '{table_name}' does not exist. Creating the table...")
        
        # Write the DataFrame as a new table in the Lakehouse
        df_with_id.write.format("delta").mode("overwrite").saveAsTable(table_name)
        
        print(f"Table '{table_name}' created with  uuid column .")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run Functions In parallel

# CELL ********************

from typing import Callable, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_functions_in_parallel(*functions: Callable[[], Any]) -> List:
    """
    Runs multiple functions in parallel, ensuring each function has isolated scope.
    
    Parameters:
    *functions: Callable
        Variable number of functions to be executed in parallel.
        
    Returns:
    List:
        A list of results from each function, in the order they were passed in.
    """
    # Using ThreadPoolExecutor to manage parallel execution
    with ThreadPoolExecutor() as executor:
        # Submit each function to the executor, encapsulating each call to ensure isolation
        futures = [executor.submit(func) for func in functions]
        
        # Collect results in order of submission, ensuring each is isolated in its execution
        results = []
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                results.append(e)  # Capture exceptions for any failed function

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load and Print Columns

# CELL ********************

def load_and_print_columns(file_path: str, columns: Optional[List[str]] = None, num_rows: int = 20) -> None:
    """
    Loads a Parquet file into a PySpark DataFrame, selects specific columns (if provided),
    and prints a specified number of rows.

    :param file_path: The path to the Parquet file
    :param columns: Optional list of columns to select. If None, selects all columns.
    :param num_rows: The number of rows to display (default is 20)
    """
    # Load data from the Parquet file
    df = spark.read.parquet(file_path)

    # Select specified columns or all columns if none are specified
    if columns is not None:
        selected_df = df.select(*columns)  # Unpack the list of columns
    else:
        selected_df = df  # Select all columns by default

    # Display the specified number of rows without truncating the output
    selected_df.show(num_rows, truncate=False) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def bridge_table_maker(df: DataFrame, lakehouse_table: str):
    """
    Merges a DataFrame with an existing Delta table in the lakehouse if it exists,
    removes duplicates, and saves the resulting DataFrame in overwrite mode.

    Parameters:
    - df (DataFrame): The DataFrame to merge with the existing table.
    - lakehouse_table (str): The name of the table in the lakehouse.
    """
    try:
        # Check if the table exists
        if DeltaTable.isDeltaTable(spark, lakehouse_table):
            print(f"Table '{lakehouse_table}' exists. Loading existing data for union.")
            
            # Load existing table
            existing_table_df = spark.table(lakehouse_table)
            
            # Union the two DataFrames and get distinct rows
            combined_df = df.union(existing_table_df).distinct()
            print(f"Unioned DataFrame has {combined_df.count()} rows after deduplication.")
        else:
            print(f"Table '{lakehouse_table}' does not exist. Proceeding with the provided DataFrame.")
            combined_df = df.distinct()

        # Save the resulting DataFrame to the lakehouse in overwrite mode
        saveDataFrameToLakeHouse(lakehouse_table,df)

    except Exception as e:
        print(f"An error occurred while processing the table '{lakehouse_table}': {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
def saveDataFrameToLakeHouse(tableName: str, dF: DataFrame):
    """
    Save a DataFrame to a specified Delta table path in the lake house with schema evolution.
    Writes to a temporary table suffixed with `_temp` and then swaps it with the existing table if it exists.
    
    Parameters:
    - tableName: Name of the main table to save.
    - dF: The DataFrame to be saved.
    """
    # Define the temporary table name
    temp_table_name = f"{tableName}_temp"
    
    # Step 1: Write the DataFrame to the temporary table
    try:
        dF.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(temp_table_name)
        print(f"Data written successfully to temporary table {temp_table_name}")
    except Exception as e:
        print(f"Error writing data to temporary table {temp_table_name}: {e}")
        return

    # Step 2: Check if the main table exists using a catalog query
    try:
        spark.sql(f"DESCRIBE TABLE {tableName}")
        main_table_exists = True
    except AnalysisException:
        main_table_exists = False

    print(f"Checked for existing table. Table exists: {main_table_exists}")

    # Step 3: If the main table exists, archive it, then rename the temp table
    if main_table_exists:
        try:
            # Archive the existing table
            print(f"Archiving the existing table {tableName} as archive_{tableName}")
            spark.sql(f"ALTER TABLE {tableName} RENAME TO archive_{tableName}")
            print(f"Existing table archived as archive_{tableName}")
        except Exception as e:
            print(f"Error archiving existing table {tableName}: {e}")
            return

    # Rename the temporary table to the main table
    try:
        print(f"Renaming {temp_table_name} to {tableName}")
        spark.sql(f"ALTER TABLE {temp_table_name} RENAME TO {tableName}")
        print(f"Temporary table {temp_table_name} renamed to {tableName}")
        
        # Drop the archive table after swap
        if main_table_exists:
            print(f"Dropping the archived table archive_{tableName}")
            spark.sql(f"DROP TABLE IF EXISTS archive_{tableName}")
            print(f"Archived table archive_{tableName} dropped.")
    except Exception as e:
        print(f"Error renaming temporary table {temp_table_name} to {tableName}: {e}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Display Data frame

# CELL ********************

from pyspark.sql import DataFrame

def displayDataFrame(df: DataFrame, recordlimit = 1000):
    """
    Converts a PySpark DataFrame to a Pandas DataFrame and displays it interactively.
    This approach is useful in notebook environments like Jupyter or Databricks
    where Pandas DataFrames render as adjustable, interactive tables.

    Parameters:
    - df (DataFrame): The PySpark DataFrame to convert and display.

    Returns:
    - None: Displays the DataFrame interactively within the notebook.
    """
    # Convert the PySpark DataFrame to a Pandas DataFrame
    # This allows for interactive viewing in notebook environments
    pandas_df = df.limit(recordlimit).toPandas()

    # Display the Pandas DataFrame as an interactive table
    display(pandas_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
