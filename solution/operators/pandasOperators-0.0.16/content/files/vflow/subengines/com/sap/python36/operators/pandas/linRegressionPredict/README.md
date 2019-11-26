# Linear Regression Predict DataFrame
Using the model calculated with the Scikit Learn module to predict values. 

## Input
* **inData**
* **inCoef**

## Output
* **outDataMsg** -message.DataFrame-
* **Info** -string-

## Config
* **regression_cols_value** -string-- list of comma-separated maps with columns and values that overrides the prediction data of the *inData* message. Only applicable for fixe values. Otherwise the *inData* message needs to be used. 
* **prediction_col_only** -boolean- outDataMsg contains only the segment columns and the prediction columns if *True* otherwise all columns

## Pandas Base


## Additional package
[ScitLearn Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)