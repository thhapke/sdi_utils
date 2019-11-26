# Linear Regression Train DataFrame
Using Scikit Learn module to train a linear regression model.


## Input
* ** inDataFrame**

## Output
* **outDataMsg** -message.DataFrame-
* **outCoefMsg** -message.DataFrame-
* **Info** -string-

## Config
* **segment_cols** -string- The segments as a comma-separated list of columns for which independantly a regression is applied, e.g. "'zip', 'city'"
* **regression_cols** -string-- The columns used for calculating the linear fit as a list of  comma-separated columns 
* **prediction_col** -string- Column for which the regession does the predicition


## Pandas Base


## Additional package
[ScitLearn Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)