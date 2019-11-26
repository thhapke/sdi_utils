# sampleDataFrame

Sampling over a DataFrame but keeps datasets with the same value of the defined column as set and not splitting them, e.g. sampling with the invariant_column='date' samples but ensures that all datasets of a certain date are taken or none. This leads to the fact that the sample_size is only a guiding target. Depending on the size of the datasets with the same value of the *invariant_column* compared to the *sample_size* this could deviate a lot. 

## Input
* **inDataFrameMsg**

## Output
* **outDataFrame**
* **Info**

## Config#
* **sample_size** -integer- size of sample
* **random_state** - integer- initializing random number generator
* **invariant_column** -string- Column that values should be kept and not split in a sample, e.g. all records of a customer should be in a sample, basically sampling over customers. Because not all the values of the invariant_columns have the same number of records the average is taken to approximate the sample_size. 

## Pandas Base

[pandas doc: sample](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sample.html)
	
