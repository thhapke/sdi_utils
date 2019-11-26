# groupbyDataFrame
Groups the named columns by using the given aggregations.  

## Input
* ** outDataFrame**

## Output
* **outDataFrame**
* **Info**

## Config
* **groupby** -string-  List of comma separated columns to group
* **aggregation** -string-- List of comma separated mappings of columns with the type of aggregation, e.g. 'price':'mean','city':'count'
* **index** -boolean-  
* **drop_columns** -string- List of columns of the joined DataFrame that could be dropped. 

## Pandas Base
[pandas doc: .grouby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)


