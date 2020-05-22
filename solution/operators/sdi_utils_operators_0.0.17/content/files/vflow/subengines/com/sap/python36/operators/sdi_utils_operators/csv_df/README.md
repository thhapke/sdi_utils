# fromCSV - sdi_pandas.fromCSV (Version: 0.0.17)

Creating a DataFrame with csv-data passed through inport.

## Inport

* **inCSVMsg** (Type: message) 

## outports

* **Info** (Type: string) 
* **outDataFrameMsg** (Type: message.DataFrame) 

## Config

* **index_cols** - Index Columns (Type: string) Index columns of dataframe
* **separator** - Separator of CSV (Type: string) Separator of CSV
* **error_bad_lines** - Error on bad lines (Type: boolean) When True raises error on bad lines
* **use_columns** - Use columns from CSV (Type: string) Use columns from CSV (list)
* **limit_rows** - Limit number of rows (Type: number) Limit number of rows for testing purpose
* **downcast_int** - Downcast integers (Type: boolean) Downcast integers from int64 to int with smallest memory footprint
* **downcast_float** - Downcast float datatypes (Type: boolean) Downcast float64 to float32 datatypes
* **df_name** - DataFrame name (Type: string) DataFrame name for debugging reasons
* **low_memory** - Low Memory  (Type: boolean) Low Memory Flag
* **thousands** - Thousands separator (Type: string) Thousands separator
* **decimal** - Decimals separator (Type: string) Decimals separator
* **compression** - Compression Format (Type: string) Compression Format
* **dtypes** - Data Types of Columns (Type: string) Data Types of Columns (list of maps)
* **data_from_filename** - Data from Filename (Type: string) Data from Filename
* **todatetime** - To Datetime (Type: string) To Datetime


# Tags
pandas : 

# References
[pandas doc: read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)

