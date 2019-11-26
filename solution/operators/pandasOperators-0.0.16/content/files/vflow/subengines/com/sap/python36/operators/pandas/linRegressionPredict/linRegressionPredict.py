import pandas as pd
import numpy as np
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5

def process(msg,coef_msg):

    prev_att = msg.attributes
    df = msg.body
    coef_df = coef_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation
    # segment columns
    segment_cols = None
    if 'segmentation_columns' in coef_msg.attributes :
        segment_cols = coef_msg.attributes['segmentation_columns']

    # regression columns
    regression_cols = coef_msg.attributes['regression_columns']

    # prediction column
    prediction_col = coef_msg.attributes['prediction_column']

    # setting values of regression column values (if not in the dataMsg already done
    att_dict['config']['regresssion_cols_value'] = api.config.regresssion_cols_value
    valmap = tfp.read_dict(api.config.regresssion_cols_value)
    if valmap :
        for col,val in valmap.items() :
            if np.issubdtype(df[col].dtype, np.integer) :
                val = int(val)
            elif np.issubdtype(df[col].dtype, np.float) :
                val = float(val)
            else :
                raise ValueError('Regression value needs to be numeric')
            df[col] = val

    # merge data and coef df
    if segment_cols :
        df = pd.merge(df,coef_df,how='inner',left_on=segment_cols,right_on=segment_cols)

    prefix = tfp.read_value(api.config.prediction_prefix)
    if prefix == None :
        prefix = ''
    pcol = prefix + prediction_col

    if segment_cols :
        def predict(x) :
            x[pcol] = np.dot(x['coef'],x[regression_cols].values) + x['intercept']
            return x
        df = df.apply(predict, axis=1, result_type=None)
        df.drop(columns=['coef', 'intercept'], inplace=True)
    else :
        def predict(x):
            x[pcol] = np.dot(coef_df['coef'],x[regression_cols].values) + coef_df['intercept']
            return x
        df = df.apply(predict,axis = 1,result_type = None)

    # cast type of prediction col from prediction variable
    if df[prediction_col].dtype == np.integer :
        df[pcol] = df[pcol].round().astype(df[prediction_col].dtype)

    if api.config.prediction_col_only :
        if segment_cols :
            df[prediction_col] = df[pcol]
            df = df[segment_cols + [prediction_col]]
        else :
            df = df[prediction_col]
    att_dict['config']['prediction_col_only'] = api.config.prediction_col_only

    #print(df[[pcol, prediction_col]].head(5))
    #print(df.head(10))

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'regressionTrainingDataFrame'
    att_dict['name'] = prev_att['name']
    #att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    #att_dict['columns'] = str(list(df.columns))
    #att_dict['number_columns'] = df.shape[1]
    #att_dict['number_rows'] = df.shape[0]

    #example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    #for i in range(0,example_rows) :
    #    att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    SIMPLE = 0

actual_test = test.SIMPLE

try:
    api
except NameError:
    class api:

        def get_default_input():
            data_df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2,3,4,5,6], 'col4': [5,6.5,7.5,8,9],\
                 'col5': [6, 6.7, 8.2, 9, 10.1]})
            att_data = {'format': 'panda', 'name': 'DF_name'}
            # without segmentation
            coef_df = pd.Series({'coef':[0.57, 0.57,-0.09],'intercept':4.67})
            att_coef = {'format': 'panda', 'name': 'Coeff', 'regression_columns': ['col2', 'col3', 'col4'], \
                        'prediction_column': 'col5'}
            # with segmentation
            coef_df = pd.DataFrame({'icol':[1,3],'coef': [[0.165, 0.165, 0.25],[0.25, 0.25, 0.6]], 'intercept': [4.27,1.95]})
            att_coef = {'format': 'panda', 'name': 'Coeff', 'regression_columns': ['col2', 'col3', 'col4'], \
                        'prediction_column': 'col5','segmentation_columns':['icol']}

            return api.Message(attributes=att_data,body=data_df),api.Message(attributes=att_coef,body=coef_df)

        class config:
            prediction_col_only = False
            regresssion_cols_value = 'None'
            prediction_prefix = 'p_'

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(10))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            msg_data, msg_coef = api.get_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + str(port) + "\"..")
            callback(msg_data,msg_coef)

        def call(data_msg,coef_msg,config):
            api.config = config
            msg = process(data_msg,coef_msg)
            return msg, json.dumps(msg.attributes, indent=4)


def interface(msg,coef_df):
    msg = process(msg,coef_df)
    api.send("outDataMsg", msg)
    info_str = json.dumps(msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback(["inDataMsg","inCoefMsg"], interface)

