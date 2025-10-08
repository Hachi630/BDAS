# PySpark Online Retail II Dataset Analysis

This project provides complete code for analyzing the Online Retail II dataset using PySpark in Google Colab.

## Troubleshooting

### PySpark Connection Issues in Local Environment

If you encounter `Py4JJavaError` or `SocketTimeoutException` when running PySpark locally, try these solutions:

#### Solution 1: Use the Enhanced Configuration
The updated code includes better Spark configurations for local environments:
- Increased worker timeout
- Memory optimization
- Arrow optimization disabled for compatibility

#### Solution 2: Use Pandas-Only Version
If PySpark continues to fail, use the pandas-only analysis:
- Run the "Alternative: Pandas-Only Analysis" section in the notebook
- Or use `pyspark_online_retail_analysis_local.py` script

#### Solution 3: Environment Setup
Ensure you have the correct Java version:
```bash
# Check Java version (should be Java 8 or 11)
java -version

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java
```

#### Solution 4: Install Required Dependencies
```bash
pip install pyspark pandas openpyxl pyarrow
```

## File Description

- `pyspark_online_retail_analysis.py` - Complete Python script version (enhanced for local environments)
- `pyspark_online_retail_analysis_local.py` - Pandas-only version for environments where PySpark fails
- `Online_Retail_II_Analysis.ipynb` - Jupyter Notebook version with error handling and pandas fallback
- `online_retail_II.xlsx` - Original dataset file

## Steps to Use in Google Colab

### 1. No File Upload Required
The code automatically loads the dataset from GitHub repository:
- Username: `Hachi630`
- Repository: `BDAS`
- File: `online_retail_II.xlsx`

### 2. Run Notebook
Open the `Online_Retail_II_Analysis.ipynb` file and run all cells in sequence.

### 3. Code Features

The code implements the following features:

#### Initialize Spark Session
- Configure Spark session to work properly in Colab
- Set appropriate log levels

#### Data Loading
- Automatically load Excel file from GitHub repository (Hachi630/BDAS)
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

- No need to upload dataset file - automatically loads from GitHub
- Ensure necessary packages are installed: `pyspark`, `pandas`, `openpyxl`
- Code includes detailed English comments for easy understanding of each step
- All analysis uses PySpark's distributed computing capabilities
- GitHub URL: `https://raw.githubusercontent.com/Hachi630/BDAS/main/online_retail_II.xlsx`

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
