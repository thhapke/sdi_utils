# transposeColumnDataFrame
Transposes the values of a column to new columns with the name of the values. The values are taken from the value_column. The labels of the new columns are a concatination ot the *transpose_column* and the value. *transpose_column* and *value_column*  are dropped. 

## Input
* ** outDataFrame**

## Output
* **outDataFrame**
* **Info**

## Config
* **transpose_column** -string-  column label which values are used for transposing
* **value_column** -string-  column label which values are used as the values for the transposed columns
* **aggr_trans** -string-- aggregation for groupby for transpose_column
* **aggr_default** -string- default aggregation for groupby
* **reset_index** -boolean-  
* **as_index** -string- column label that is the new index
* **groupby** -string- 

## Pandas Base
[pandas doc: .groupby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)


