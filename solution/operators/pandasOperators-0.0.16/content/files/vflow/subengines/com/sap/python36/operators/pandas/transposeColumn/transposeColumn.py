import pandas as pd
import json

import textfield_parser.textfield_parser as tfp

MAX_COLUMNS = 2

def process(msg):

    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    att_dict['config']['reset_index'] = api.config.reset_index
    if api.config.reset_index:
        df.reset_index(inplace=True)

    # create DataFrame with numbered columns add concat it to df
    att_dict['config']['transpose_column'] = api.config.transpose_column
    trans_col = tfp.read_value(api.config.transpose_column)

    att_dict['config']['value_column'] = api.config.value_column
    val_col = tfp.read_value(api.config.value_column)

    # new columns
    tvals = list(df[trans_col].unique())
    if api.config.prefix :
        new_cols = {trans_col + '_' + str(v): v for v in tvals}
    else :
        new_cols = {str(v): v for v in tvals}
    t_df = pd.DataFrame(columns=new_cols.keys(), index=df.index)
    df = pd.concat([df, t_df], axis=1)

    # setting the corresponding column to the value of the value column
    for col, val in new_cols.items():
        df.loc[df[trans_col] == val, col] = df.loc[df[trans_col] == val, val_col]
    df.drop(columns=[trans_col,val_col], inplace=True)

    att_dict['config']['groupby'] = api.config.groupby
    gbcols = tfp.read_list(api.config.groupby,df.columns)
    # group df
    if gbcols :
        aggr_trans = api.config.aggr_trans.strip()
        aggr_default = api.config.aggr_default.strip()

        aggregation = dict()
        for col in df.columns:
            aggregation[col] = aggr_trans if col in new_cols else aggr_default
        aggregation = { c:a for c,a in aggregation.items() if c not in gbcols }

        df = df.groupby(gbcols,as_index = api.config.as_index).agg(aggregation)


    #####################
    #  final infos to attributes and info message
    #####################

    # df from body
    att_dict['operator'] = 'transposeColumnDataFrame' # name of operator
    att_dict['mem_usage'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['name'] = prev_att['name']
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = len(att_dict['columns'])
    att_dict['number_rows'] = len(df.index)
    att_dict['example_row_1'] = str(df.iloc[0, :].tolist())

    return api.Message(attributes=att_dict,body = df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''
class test :
    BIGDATA = 1
    SIMPLE = 0

test_scenario = test.SIMPLE

try:
    api
except NameError:
    class api:

        # input data - only used for isolated testing
        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 2, 2, 5, 5], 'xcol2': [1, 2, 2, 2, 3], 'xcol3': ['A', 'B', 'B', 'B', 'C'], \
                 'xcol4': ['L', 'L', 'K', 'N', 'C']})

            # input data
            att = {'format': 'pandas','name':'test'}

            return api.Message(attributes=att,body=df)


        # definition of api.config - variable names should be same as in DI implementation
        class config:
            transpose_column = 'xcol3'
            value_column = 'xcol2'
            groupby = 'icol'
            aggr_trans = 'sum'
            aggr_default = 'first'
            reset_index = False
            as_index = False
            prefix = False

        # fake definition of api.Message
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        # fake definition - can be used of asserting test results
        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(10))
            #else :
            #    print(msg)
            pass

        # fake definition - called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            msg = api.get_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


# gateway that gets the data from the inports and sends the result to the outports
def interface(msg):
    result= process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)

# Triggers the request for every message
api.set_port_callback("inDataFrameMsg", interface)
