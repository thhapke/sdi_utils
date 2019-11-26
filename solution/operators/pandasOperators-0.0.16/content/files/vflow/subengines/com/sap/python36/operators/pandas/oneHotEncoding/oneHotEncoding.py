import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
import re
import json


import textfield_parser

EXAMPLE_ROWS = 5

def get_value_list(param_str,val_list) :
    # only a list with a leading modifier is needed
    param_str_clean = param_str.replace(':', '').replace('=', '')
    # Test for ALL
    result = re.match(r'^([Aa][Ll][Ll])\s*$', param_str_clean)
    if result :
        return val_list
    # Test for NOT
    result = re.match(r'^([Nn][Oo][Tt])(.+)', param_str_clean)
    if result and result.group(1).upper() == 'NOT':
        exclude_values = [x.strip().strip("'").strip('"') for x in result.group(2).split(',')]
        ret_val = [x for x in val_list if x not in exclude_values]
    else:
        ret_val = [x.strip().strip("'").strip('"') for x in param_str_clean.split(',')]
    return ret_val


def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation
    #enc = OneHotEncoder(handle_unknown='ignore')
    #if api.config.training_cols and not api.config.training_cols.upper() ==  'NONE' :
    #    train_cols = get_value_list(api.config.training_cols,df.columns)
    #    df = pd.DataFrame(enc.fit_transform(df[train_cols]))
    #att_dict['config']['training_cols'] = api.config.training_cols

    df = pd.get_dummies(df,prefix_sep='_',drop_first=True)


    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

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

        def set_test(test_scenario):
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            pass

        class config:
            training_cols = 'None'

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
            msg = api.set_test(actual_test)
            api.set_config(actual_test)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback("inDataFrameMsg", interface)

