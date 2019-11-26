import pandas as pd
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    # groupby list
    cols = tfp.read_list(api.config.groupby)
    att_dict['config']['groupby'] = api.config.groupby

    # mapping
    colagg = tfp.read_dict(api.config.aggregation)
    att_dict['config']['aggregation'] = api.config.aggregation

    # groupby
    df = df.groupby(cols, as_index=api.config.index).agg(colagg)

    # drop col
    att_dict['config']['dropcols'] = api.config.drop_columns
    dropcols = tfp.read_list(api.config.drop_columns)
    if dropcols :
        df.drop(columns=dropcols,inplace=True)

    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['operator'] = 'groupbyDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

try:
    api
except NameError:
    class api:

        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 1, 1, 1, 2], 'xcol 2': ['A', 'A', 'B', 'B', 'C'], 'xcol 3': [1, 1,2,2,3],'xcol4': ['a', 'a','b','a','b']})
            attributes = {'format': 'csv','name':'DF_name'}
            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            if test_scenario == test.PORTAL_1:
                api.config.groupby = "'Exportdatum', 'Postleitzahl', 'Ort', 'Verbrauchsstufe','Rang'" # list
                api.config.aggregation = "'Gesamtpreis':'mean','Ortsteil':'count'"  # map key:value
                api.config.index = True
                api.config.drop_columns = "'Ortsteil'"
            else : # SIMPLE'
                api.config.groupby = "'icol', 'xcol 2'"  # list
                api.config.aggregation = "'xcol 3':'sum','xcol4':'count'"  # map key:value
                api.config.index = False
                api.config.drop_columns = "'xcol4'"

        class config:
            groupby = "'icol', 'xcol 2'"  # list
            aggregation = "'xcol 3':'sum','xcol4':'count'"  # map key:value
            index = False
            drop_columns = "'xcol4'"

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(100))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            msg = api.get_default_input()
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
api.set_port_callback("inDataFrameMsg", interface)

