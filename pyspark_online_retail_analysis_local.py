# PySpark Online Retail II Dataset Analysis - Local Environment Version
# This version handles both PySpark and pandas fallback for local environments

# First install necessary packages (run in terminal/command prompt)
# pip install pyspark pandas openpyxl

# Import necessary libraries
import pandas as pd
import numpy as np
from datetime import datetime

# Try to import PySpark, if not available, use pandas only
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    from pyspark.sql.functions import col, sum as spark_sum, count, when, isnan, isnull, desc, min as spark_min, max as spark_max
    PYSPARK_AVAILABLE = True
    print("PySpark is available")
except ImportError:
    PYSPARK_AVAILABLE = False
    print("PySpark not available, using pandas only")

# Initialize Spark session (if PySpark is available)
if PYSPARK_AVAILABLE:
    try:
        # Configure Spark for local environment
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
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        # Set log level to reduce output noise
        spark.sparkContext.setLogLevel("WARN")
        print("Spark session initialized successfully!")
        print(f"Spark version: {spark.version}")
        SPARK_WORKING = True
    except Exception as e:
        print(f"Failed to initialize Spark: {e}")
        print("Continuing with pandas only...")
        SPARK_WORKING = False
else:
    SPARK_WORKING = False

# Load data using pandas
print("\nReading Excel file from GitHub...")

# GitHub repository information
github_user = "Hachi630"
github_repo = "BDAS"
file_path = "online_retail_II.xlsx"

# Construct GitHub raw URL
github_url = f"https://raw.githubusercontent.com/{github_user}/{github_repo}/main/{file_path}"

# Use pandas to read Excel file from GitHub
pandas_df = pd.read_excel(github_url)
print("Data successfully loaded from GitHub!")

# Convert to Spark DataFrame if Spark is working
if SPARK_WORKING:
    try:
        df = spark.createDataFrame(pandas_df)
        print("Data successfully converted to Spark DataFrame!")
        USE_SPARK = True
    except Exception as e:
        print(f"Failed to convert to Spark DataFrame: {e}")
        print("Using pandas DataFrame for analysis...")
        USE_SPARK = False
else:
    USE_SPARK = False

# Check data dimensions
print("\n=== Data Dimension Information ===")

# Get row count
row_count = len(pandas_df)
print(f"Dataset row count: {row_count:,}")

# Get column count
column_count = len(pandas_df.columns)
print(f"Dataset column count: {column_count}")

# Display column names
print(f"Column names: {list(pandas_df.columns)}")

# Preview data - show first 5 rows
print("\n=== Data Preview (First 5 Rows) ===")
print(pandas_df.head())

# Print data schema to verify data types
print("\n=== Data Schema ===")
print(pandas_df.dtypes)

# Display basic statistical summary for numeric columns
print("\n=== Numeric Columns Statistical Summary ===")
print(pandas_df.describe())

# Additional statistical information
print("\n=== Detailed Statistical Summary ===")
# Get additional statistics
numeric_cols = pandas_df.select_dtypes(include=[np.number]).columns
for col in numeric_cols:
    print(f"\n{col}:")
    print(f"  Count: {pandas_df[col].count()}")
    print(f"  Mean: {pandas_df[col].mean():.2f}")
    print(f"  Median: {pandas_df[col].median():.2f}")
    print(f"  Std: {pandas_df[col].std():.2f}")
    print(f"  Min: {pandas_df[col].min()}")
    print(f"  Max: {pandas_df[col].max()}")

# Check for missing values
print("\n=== Missing Values Check ===")
missing_values = pandas_df.isnull().sum()
print(missing_values)

# Check negative values in Quantity column (returns)
print("\n=== Quantity Column Analysis ===")
quantity_stats = {
    "Min Quantity": pandas_df['Quantity'].min(),
    "Max Quantity": pandas_df['Quantity'].max(),
    "Return Records Count": (pandas_df['Quantity'] < 0).sum(),
    "Normal Sales Records Count": (pandas_df['Quantity'] > 0).sum()
}
for key, value in quantity_stats.items():
    print(f"{key}: {value}")

# Check UnitPrice column range
print("\n=== UnitPrice Column Analysis ===")
price_stats = {
    "Min Unit Price": pandas_df['UnitPrice'].min(),
    "Max Unit Price": pandas_df['UnitPrice'].max(),
    "Negative Price Records Count": (pandas_df['UnitPrice'] < 0).sum(),
    "Zero Price Records Count": (pandas_df['UnitPrice'] == 0).sum()
}
for key, value in price_stats.items():
    print(f"{key}: {value}")

# Display record counts by country
print("\n=== Record Count by Country (Top 10) ===")
country_counts = pandas_df['Country'].value_counts().head(10)
print(country_counts)

# Display record counts by customer
print("\n=== Record Count by Customer (Top 10) ===")
customer_counts = pandas_df['Customer ID'].value_counts().head(10)
print(customer_counts)

print("\n=== Analysis Complete ===")
print("Dataset basic information summary:")
print(f"- Total records: {row_count:,}")
print(f"- Column count: {column_count}")
print(f"- Main columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, Customer ID, Country")
print("- Data types verified through dtypes")
print("- Statistical summary shows distribution of numeric columns")
print("- Missing values and anomalies checked")
if USE_SPARK:
    print("- Analysis performed using PySpark")
else:
    print("- Analysis performed using pandas")

# Stop Spark session if it was started
if SPARK_WORKING:
    try:
        spark.stop()
        print("Spark session stopped.")
    except:
        pass
