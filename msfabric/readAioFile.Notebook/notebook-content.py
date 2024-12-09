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
# META       "default_lakehouse_workspace_id": "ace43e1b-866a-4e00-a24b-b9997ff19815"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder.appName("Process Large CSV").getOrCreate()

# Step 2: Define file paths
input_file_path = "Files/raw/qualys2/qualysAllInOne.csv"
output_file_path = "Files/raw/qualys2/processed_output2.csv"

# Step 3: Read the file line by line using RDD
raw_rdd = spark.sparkContext.textFile(input_file_path)

# Step 4: Remove all line breaks (carriage returns, line feeds) and process each line
processed_rdd = raw_rdd.map(lambda line: line.replace("\r", "").replace("\n", "")) \
                       .flatMap(lambda line: line.split("~~~"))  # Split by `~~~` to create new rows

# Step 5: Coalesce to ensure a single output file
processed_rdd = processed_rdd.coalesce(1)

# Step 6: Save the processed RDD to a new CSV file
processed_rdd.saveAsTextFile(output_file_path)

# Verify by loading a sample of the processed file
print("Processing complete. Sample output:")
sample_output = processed_rdd.take(10)
for row in sample_output:
    print(row)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# File path
file_path = "abfss://fab_kpscom@onelake.dfs.fabric.microsoft.com/lhKpScom.Lakehouse/Files/raw/qualys2/processed_output2.csv"

# Read the CSV file with the custom delimiter $x$
df = pd.read_csv(file_path, delimiter="$x$", engine="python")

# Display the DataFrame
print("DataFrame loaded successfully:")
print(df.head())

# (Optional) Display column names
print("\nColumns in the DataFrame:")
print(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
