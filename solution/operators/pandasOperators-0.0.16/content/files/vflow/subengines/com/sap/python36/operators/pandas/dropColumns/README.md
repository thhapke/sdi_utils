# dropColumns
Drops or/and renames DataFrame columns

## Input
* **inDataFrameMsg**

## Output
* **outDataFrame**
* **Info**

## Config
* **drop_columns** 
	* *comma separated list of columns*: columns to drop
	* *NOT: comma separated list of columns*: drop all columns except columns in the list 
	* *ALL* : drop all columns and reset index - same as *NOT* 
* **rename_columns**
	*  *comma separated list of mappings*: columns to be renamed, e.g. Col1:col_1, Col2:col_2

## Pandas Base

[pandas doc: drop](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html)

[pandas doc: rename](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html)

