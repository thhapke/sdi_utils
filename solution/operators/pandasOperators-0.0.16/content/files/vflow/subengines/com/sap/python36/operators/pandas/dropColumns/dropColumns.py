import pandas as pd
import re
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5


def process(msg):

    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation
    att_dict['config']['drop_columns'] = api.config.drop_columns
    drop_cols = tfp.read_list(api.config.drop_columns,df.columns)
    if drop_cols :
        df = df.drop(columns=drop_cols)

    att_dict['config']['rename_columns'] = api.config.rename_columns
    map_names = tfp.read_dict(api.config.rename_columns)
    if map_names :
        df.rename(columns = map_names, inplace=True)
    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    # df from body
    att_dict['operator'] = 'dropColumns' # name of operator
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['name'] = prev_att['name']
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = len(att_dict['columns'])
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return api.Message(attributes=att_dict,body = df)



'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

try:
    api
except NameError:
    class api:

        # input data - only used for isolated testing
        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'B', 'C', 'D', 'E'], \
                 'xcol3': ['K', 'L', 'M', 'N', 'O'],'xcol4': ['a1', 'a1', 'b1', 'b1', 'b1'], \
                 'xcol5': ['c1', 'c1', 'e1', 'e1', 'e1']})
            attributes = {'format': 'df', 'name': 'DF_name'}
            return api.Message(attributes=attributes, body=df)

        # definition of api.config - variable names should be same as in DI implementation
        class config:
            drop_columns = 'xcol3'
            rename_columns = 'xcol4 : colum4, xcol5: column5'

        # fake definition of api.Message
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        # fake definition - can be used of asserting test results
        def send(port, msg):
            if isinstance(msg,str) :
                print(msg)
            else :
                print(msg.body)
            pass

        # fake definition - called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            msg = api.get_default_input()
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)

# gateway that gets the data from the inports and sends the result to the outports
def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message
#api.set_port_callback("inDataFrameMsg", interface)
