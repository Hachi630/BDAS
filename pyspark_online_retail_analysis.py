# PySpark Online Retail II Dataset Analysis
# Load and analyze Online Retail II dataset using PySpark in Google Colab

# First install necessary packages (run in Colab)
# !pip install pyspark pandas openpyxl

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum, count, when, isnan, isnull, desc, min as spark_min, max as spark_max
import pandas as pd

# Initialize Spark session
# Configure Spark for both local and Colab environments
spark = SparkSession.builder \
    .appName("OnlineRetailAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
    .config("spark.python.worker.timeout", "300") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

# Set log level to reduce output noise
spark.sparkContext.setLogLevel("WARN")

print("Spark session initialized successfully!")
print(f"Spark version: {spark.version}")

# Since PySpark cannot directly read Excel files, we use pandas to read and then convert to Spark DataFrame
print("\nReading Excel file from GitHub...")

# GitHub repository information
github_user = "Hachi630"
github_repo = "BDAS"
file_path = "online_retail_II.xlsx"

# Construct GitHub raw URL
github_url = f"https://raw.githubusercontent.com/{github_user}/{github_repo}/main/{file_path}"

# Use pandas to read Excel file from GitHub
pandas_df = pd.read_excel(github_url)

# Convert pandas DataFrame to Spark DataFrame
# Ensure DataFrame is named df for consistency
df = spark.createDataFrame(pandas_df)

print("Data successfully loaded from GitHub into Spark DataFrame!")

# Check data dimensions
print("\n=== Data Dimension Information ===")
# Get row count with error handling
try:
    row_count = df.count()
    print(f"Dataset row count: {row_count:,}")
except Exception as e:
    print(f"Error getting row count with Spark: {e}")
    print("Using pandas DataFrame for row count...")
    row_count = len(pandas_df)
    print(f"Dataset row count (from pandas): {row_count:,}")

# Get column count
column_count = len(df.columns)
print(f"Dataset column count: {column_count}")

# Display column names
print(f"Column names: {df.columns}")

# Preview data - show first 5 rows
print("\n=== Data Preview (First 5 Rows) ===")
df.show(5, truncate=False)

# Print data schema to verify data types
print("\n=== Data Schema ===")
df.printSchema()

# Display basic statistical summary for numeric columns
print("\n=== Numeric Columns Statistical Summary ===")
# Use describe() method to get statistical information for numeric columns
df.describe().show()

# Additional statistical information - use summary() method for more detailed statistics
print("\n=== Detailed Statistical Summary ===")
df.summary().show()

# Check for missing values
print("\n=== Missing Values Check ===")

# Calculate missing value count for each column
missing_values = df.select([spark_sum(when(isnull(c) | isnan(c), 1).otherwise(0)).alias(c) for c in df.columns])
missing_values.show()

# Check negative values in Quantity column (returns)
print("\n=== Quantity Column Analysis ===")

quantity_stats = df.select(
    spark_min("Quantity").alias("Min Quantity"),
    spark_max("Quantity").alias("Max Quantity"),
    count(when(col("Quantity") < 0, 1)).alias("Return Records Count"),
    count(when(col("Quantity") > 0, 1)).alias("Normal Sales Records Count")
)
quantity_stats.show()

# Check UnitPrice column range
print("\n=== UnitPrice Column Analysis ===")
price_stats = df.select(
    spark_min("UnitPrice").alias("Min Unit Price"),
    spark_max("UnitPrice").alias("Max Unit Price"),
    count(when(col("UnitPrice") < 0, 1)).alias("Negative Price Records Count"),
    count(when(col("UnitPrice") == 0, 1)).alias("Zero Price Records Count")
)
price_stats.show()

# Display record counts by country
print("\n=== Record Count by Country (Top 10) ===")
df.groupBy("Country").count().orderBy(desc("count")).show(10)

# Display record counts by customer
print("\n=== Record Count by Customer (Top 10) ===")
df.groupBy("Customer ID").count().orderBy(desc("count")).show(10)

print("\n=== Analysis Complete ===")
print("Dataset basic information summary:")
print(f"- Total records: {row_count:,}")
print(f"- Column count: {column_count}")
print(f"- Main columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, Customer ID, Country")
print("- Data types verified through printSchema()")
print("- Statistical summary shows distribution of numeric columns")
print("- Missing values and anomalies checked")

# Stop Spark session (optional, usually not needed in Colab)
# spark.stop()
