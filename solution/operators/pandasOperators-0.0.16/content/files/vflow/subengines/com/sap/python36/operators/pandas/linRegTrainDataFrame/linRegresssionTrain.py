import pandas as pd
import json

from sklearn.linear_model import LinearRegression

import textfield_parser.textfield_parser as tfp

# setting display options for df
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 100)

EXAMPLE_ROWS = 5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    # segment columns
    att_dict['config']['segment_cols'] = api.config.segment_cols
    segment_cols = tfp.read_list(api.config.segment_cols)

    # regression columns
    att_dict['config']['regression_cols'] = api.config.regression_cols
    regression_cols = tfp.read_list(api.config.regression_cols)
    if not regression_cols :
        raise ValueError('No Regression Columns - mandatory data')

    # prediction column
    att_dict['config']['prediction_col'] = api.config.prediction_col
    prediction_col = tfp.read_value(api.config.prediction_col)
    if not prediction_col :
        raise ValueError('No Predicition Column - mandatory data')

    training_cols = regression_cols + [prediction_col]
    model = LinearRegression(fit_intercept=True)
    def fit(x) :
        model.fit(x[regression_cols], x[prediction_col])
        return pd.Series([model.coef_, model.intercept_],index=['coef','intercept'])
    if segment_cols :
        coef_df = df.groupby(segment_cols)[training_cols].apply(fit).reset_index()
    else :
        model.fit(df[regression_cols], df[prediction_col])
        coef_df = pd.Series([model.coef_, model.intercept_],index=['coef','intercept'])

    coef_att = {'segmentation_columns':segment_cols,'regression_columns':regression_cols, 'prediction_column': prediction_col}

    coef_msg = api.Message(attributes=coef_att,body=coef_df)

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'regressionTrainingDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df), coef_msg


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''
try:
    api
except NameError:
    class api:

        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9],'col5': [6, 6.7, 8.2, 9, 10.1]})

            attributes = {'format': 'pandas','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        class config:
            segment_cols = 'icol'
            regression_cols = "'col2',col3,col4"
            prediction_col = 'col5'

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                if isinstance(msg.body,pd.DataFrame) :
                    print(msg.body.head(10))

                elif isinstance(msg.body,pd.Series) :
                    print ('coef: ' + str(msg.body['coef'].round(decimals=2)))
                    print('intercept: ' + str(msg.body['intercept']))
            else :
                print(msg)
            pass

        def set_port_callback(port, callback):
            msg = api.get_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            msg, coef_msg = process(msg)
            return msg, coef_msg, json.dumps(msg.attributes, indent=4)


def interface(msg):
    msg, coef_df = process(msg)
    api.send("outDataMsg", msg)
    api.send('outCoefMsg', coef_df)
    info_str = json.dumps(msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback("inDataFrameMsg", interface)

