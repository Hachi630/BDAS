# PySpark Online Retail II Dataset Analysis - Two Tables Version
# Load and analyze Online Retail II dataset using PySpark in Google Colab

# First install necessary packages (run in Colab)
# %pip install pyspark pandas openpyxl

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

# Read Excel file with multiple sheets
print("Loading data from both sheets (2009-2010 and 2010-2011)...")
excel_data = pd.read_excel(github_url, sheet_name=None)  # Read all sheets

# Get the two sheets
sheet_2009_2010 = excel_data['Year 2009-2010']
sheet_2010_2011 = excel_data['Year 2010-2011']

print(f"2009-2010 data shape: {sheet_2009_2010.shape}")
print(f"2010-2011 data shape: {sheet_2010_2011.shape}")

# Combine both datasets
pandas_df = pd.concat([sheet_2009_2010, sheet_2010_2011], ignore_index=True)
print(f"Combined data shape: {pandas_df.shape}")

# Convert pandas DataFrame to Spark DataFrame
# Ensure DataFrame is named df for consistency
df = spark.createDataFrame(pandas_df)

print("Data successfully loaded from GitHub into Spark DataFrame!")

# Analyze individual tables
print("\n=== Individual Table Analysis ===")

print("\n--- 2009-2010 Data ---")
print(f"Shape: {sheet_2009_2010.shape}")
print(f"Columns: {list(sheet_2009_2010.columns)}")
print("First 3 rows:")
print(sheet_2009_2010.head(3))

print("\n--- 2010-2011 Data ---")
print(f"Shape: {sheet_2010_2011.shape}")
print(f"Columns: {list(sheet_2010_2011.columns)}")
print("First 3 rows:")
print(sheet_2010_2011.head(3))

print("\n--- Combined Data Summary ---")
print(f"Total records: {len(pandas_df):,}")
print(f"Total columns: {len(pandas_df.columns)}")
print(f"Columns: {list(pandas_df.columns)}")

# Check data dimensions
print("\n=== Data Dimension Information ===")
# Get row count with error handling
try:
    row_count = df.count()
    print(f"Dataset row count: {row_count:,}")
    USE_SPARK = True
except Exception as e:
    print(f"Error getting row count with Spark: {e}")
    print("Using pandas DataFrame for all analysis...")
    row_count = len(pandas_df)
    print(f"Dataset row count (from pandas): {row_count:,}")
    USE_SPARK = False

# Get column count
if USE_SPARK:
    column_count = len(df.columns)
    column_names = df.columns
else:
    column_count = len(pandas_df.columns)
    column_names = list(pandas_df.columns)

print(f"Dataset column count: {column_count}")
print(f"Column names: {column_names}")

# Preview data - show first 5 rows
print("\n=== Data Preview (First 5 Rows) ===")
if USE_SPARK:
    df.show(5, truncate=False)
else:
    print(pandas_df.head())

# Print data schema to verify data types
print("\n=== Data Schema ===")
if USE_SPARK:
    df.printSchema()
else:
    print(pandas_df.dtypes)

# Display basic statistical summary for numeric columns
print("\n=== Numeric Columns Statistical Summary ===")
if USE_SPARK:
    # Use describe() method to get statistical information for numeric columns
    df.describe().show()
else:
    print(pandas_df.describe())

# Additional statistical information - use summary() method for more detailed statistics
print("\n=== Detailed Statistical Summary ===")
if USE_SPARK:
    df.summary().show()
else:
    # Get additional statistics for numeric columns
    numeric_cols = pandas_df.select_dtypes(include=['number']).columns
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

if USE_SPARK:
    # Calculate missing value count for each column
    missing_values = df.select([spark_sum(when(isnull(c) | isnan(c), 1).otherwise(0)).alias(c) for c in df.columns])
    missing_values.show()
else:
    # Calculate missing value count for each column using pandas
    missing_values = pandas_df.isnull().sum()
    print(missing_values)

# Check negative values in Quantity column (returns)
print("\n=== Quantity Column Analysis ===")

if USE_SPARK:
    quantity_stats = df.select(
        spark_min("Quantity").alias("Min Quantity"),
        spark_max("Quantity").alias("Max Quantity"),
        count(when(col("Quantity") < 0, 1)).alias("Return Records Count"),
        count(when(col("Quantity") > 0, 1)).alias("Normal Sales Records Count")
    )
    quantity_stats.show()
else:
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
if USE_SPARK:
    price_stats = df.select(
        spark_min("UnitPrice").alias("Min Unit Price"),
        spark_max("UnitPrice").alias("Max Unit Price"),
        count(when(col("UnitPrice") < 0, 1)).alias("Negative Price Records Count"),
        count(when(col("UnitPrice") == 0, 1)).alias("Zero Price Records Count")
    )
    price_stats.show()
else:
    price_stats = {
        "Min Unit Price": pandas_df['Price'].min(),
        "Max Unit Price": pandas_df['Price'].max(),
        "Negative Price Records Count": (pandas_df['Price'] < 0).sum(),
        "Zero Price Records Count": (pandas_df['Price'] == 0).sum()
    }
    for key, value in price_stats.items():
        print(f"{key}: {value}")

# Display record counts by country
print("\n=== Record Count by Country (Top 10) ===")
if USE_SPARK:
    df.groupBy("Country").count().orderBy(desc("count")).show(10)
else:
    country_counts = pandas_df['Country'].value_counts().head(10)
    print(country_counts)

# Display record counts by customer
print("\n=== Record Count by Customer (Top 10) ===")
if USE_SPARK:
    df.groupBy("Customer ID").count().orderBy(desc("count")).show(10)
else:
    customer_counts = pandas_df['Customer ID'].value_counts().head(10)
    print(customer_counts)

print("\n=== Analysis Complete ===")
print("Dataset basic information summary:")
print(f"- Total records: {row_count:,}")
print(f"- Column count: {column_count}")
print(f"- Main columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, Customer ID, Country")
print("- Data types verified through schema")
print("- Statistical summary shows distribution of numeric columns")
print("- Missing values and anomalies checked")
if USE_SPARK:
    print("- Analysis performed using PySpark")
else:
    print("- Analysis performed using pandas (PySpark failed)")

# Stop Spark session if it was started
if USE_SPARK:
    try:
        spark.stop()
        print("Spark session stopped.")
    except:
        pass
