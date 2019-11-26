# fuzzyjoinDataFrame
A test datasets (testDataFrame) are checked if they (string-) match with a base dataset (baseDataFrame). If more than one column are provided for checking then the average is calculated of all columns. 


## Input
* ** testDataFrame**
* ** baseDataFrame**

## Output
* **outDataFrame**
* **Info**

## Config
* **check_columns** -string-  List of comma separated column mapping that will be checked correspendingly, e.g. "'company':'company_name', 'ADR':'address','city':'city'. First column refers to testDataFrame 
* **limit** -int-- Only matching higher than **limit** will tagged as matched. Value range: 0-100
* **test_index** -string- It's assumed that the testDataFrame has only an internal index. *test_index* refers to the label that can be used refering to matched dataset of test_DataFrame
* **only_index** -boolean- True, only the index of the test datasets is added as additional column. Otherwise all test columns are added with a prefix 't_'
* **only_matching_rows** -boolean- Only the datasets that have a matching are send to *outDataFrame*
* **joint_id** -boolean- A joint id is created with the base_index and the test_index. It is the *base index* unless there is a matching index
* **base_index** -string- It's assumed that the baseDataFrame has only an internal index. *base_index* refers to the label that can be used to refer to the base dataset for 'joint_id'
* **add_non_matching** -boolean- When *True* the test datasets with no matching are added to the base DataFrame


## Pandas Base
[pandas doc: .grouby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)

## Additional package
[fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)