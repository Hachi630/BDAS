# PySpark Online Retail II Dataset Analysis

This project provides complete code for analyzing the Online Retail II dataset using PySpark in Google Colab.

## File Description

- `pyspark_online_retail_analysis.py` - Complete Python script version
- `Online_Retail_II_Analysis.ipynb` - Jupyter Notebook version (recommended for use in Google Colab)
- `online_retail_II.xlsx` - Original dataset file

## Steps to Use in Google Colab

### 1. Upload Files
First, upload the `online_retail_II.xlsx` file to your Google Colab environment.

### 2. Run Notebook
Open the `Online_Retail_II_Analysis.ipynb` file and run all cells in sequence.

### 3. Code Features

The code implements the following features:

#### Initialize Spark Session
- Configure Spark session to work properly in Colab
- Set appropriate log levels

#### Data Loading
- Use pandas to read Excel file (since PySpark cannot directly read Excel)
- Convert pandas DataFrame to Spark DataFrame
- Ensure DataFrame is named `df` for consistency

#### Data Inspection
- **Dimension Check**: Use `df.count()` to get row count, `len(df.columns)` to get column count
- **Data Preview**: Use `df.show(5)` to display first 5 rows of data
- **Schema Validation**: Use `df.printSchema()` to display data types
- **Statistical Summary**: Use `df.describe().show()` and `df.summary().show()` to get statistical information

#### Data Quality Analysis
- **Missing Values Check**: Calculate missing value count for each column
- **Anomaly Detection**: Check negative values in Quantity column (returns) and anomalies in UnitPrice column
- **Group Analysis**: Perform group statistics by country and customer

## Expected Output

After running the code, you will see:

1. **Data Dimensions**: Number of rows and columns in the dataset
2. **Data Preview**: First 5 rows of data, including InvoiceNo, StockCode, Description, etc.
3. **Data Types**: Data type of each column (e.g., InvoiceDate as date type, Quantity as integer type)
4. **Statistical Summary**: Count, mean, min, max, etc. for numeric columns
5. **Data Quality Report**: Missing values, anomalies, return records, etc.

## Notes

- Ensure necessary packages are installed in Google Colab: `pyspark`, `pandas`, `openpyxl`
- Dataset file needs to be uploaded to Colab environment
- Code includes detailed English comments for easy understanding of each step
- All analysis uses PySpark's distributed computing capabilities

## Dataset Information

The Online Retail II dataset contains the following main columns:
- `InvoiceNo`: Invoice number
- `StockCode`: Product code
- `Description`: Product description
- `Quantity`: Quantity
- `InvoiceDate`: Invoice date
- `UnitPrice`: Unit price
- `Customer ID`: Customer ID
- `Country`: Country

This analysis code will help you verify the dataset structure and content, providing a foundation for subsequent data science projects.
