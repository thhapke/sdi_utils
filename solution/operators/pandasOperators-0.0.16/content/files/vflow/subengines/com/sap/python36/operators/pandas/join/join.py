import pandas as pd
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5

def process(left_msg,right_msg):

    att_dict = dict()
    att_dict['config'] = dict()

    l_att = left_msg.attributes
    r_att = right_msg.attributes

    if l_att['name'] == r_att['name'] :
        att_dict['name'] = l_att['name']
    else :
        att_dict['name'] = l_att['name'] + '-' + r_att['name']
    att_dict['config'] = dict()

    # read stream from memory
    left_df = left_msg.body
    right_df = right_msg.body

    ###### start of doing calculation
    how = tfp.read_value(api.config.how)

    # merge according to config
    att_dict['config']['on_index'] = api.config.on_index
    if api.config.on_index :
        df = pd.merge(left_df, right_df, how=how, left_index=True,right_index=True)
    elif api.config.left_on and api.config.right_on :
        att_dict['config']['left_on'] = api.config.left_on
        att_dict['config']['right_on'] = api.config.right_on

        left_on_list = tfp.read_list(api.config.left_on)
        right_on_list = tfp.read_list(api.config.right_on)
        left_df.reset_index(inplace=True)
        right_df.reset_index(inplace=True)

        df = pd.merge(left_df, right_df, how=how, left_on=left_on_list,right_on=right_on_list)

        # removing second index - might be a more elegant solution
        if 'index_x' in df.columns :
            df.drop(columns=['index_x'],inplace=True)
    else :
        raise ValueError("Config setting: Either <on> or both <left_on> and <right_on> has to be set in order to join the dataframes")

    att_dict['config']['new_indices'] = api.config.new_indices
    index_list = tfp.read_list(api.config.new_indices)
    if index_list :
        df.set_index(keys = index_list,inplace=True)

    att_dict['config']['drop_columns'] = api.config.drop_columns
    col_list = tfp.read_list(api.config.drop_columns)
    if col_list:
        df.drop(labels = col_list,axis=1,inplace=True)

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    if df.empty == True :
        raise ValueError('Merged Dataframe is empty')

    att_dict['operator'] = 'joinDataFrames'
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
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

        # input data
        def get_default_input() :
            l_df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'B', 'C', 'D', 'E'], 'xcol3': ['K', 'L', 'M', 'N', 'O']})
            l_df.set_index(keys='icol', inplace=True)
            r_df = pd.DataFrame(
                {'icol': [3, 4, 5, 6, 7], 'ycol2': ['C', 'D', 'E', 'F', 'G'], 'ycol3': ['M', 'N', 'O', 'P', 'Q']})
            r_df.set_index(keys='icol', inplace=True)

            # input data
            att1 = {'format': 'pandas','name':'leftDF'}
            att2 = {'format': 'pandas','name':'rightDF'}

            return api.Message(attributes=att1,body=l_df), api.Message(attributes=att2,body=r_df)

        class config:
            how = "inner"
            on_index = False
            left_on = "icol"
            right_on = "icol"
            new_indices = "icol"
            drop_columns = 'None'

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg,str) :
                print(msg)
            else :
                print(msg.body.head(10))
                print(msg.body.columns)
            pass

        # called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            if isinstance(port,list) :
                port = str(port)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            l_msg, r_msg = api.get_default_input()
            callback(l_msg,r_msg)

        def call(msg1,msg2,config):
            api.config = config
            result = process(msg1,msg2)
            return result, json.dumps(result.attributes, indent=4)

def interface(left_msg,right_msg):
    result_df = process(left_msg,right_msg)
    api.send("outDataFrameMsg", result_df)
    info_str = json.dumps(result_df.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
# to be commented when imported for external 'integration' call
api.set_port_callback(["leftDFMsg","rightDFMsg"], interface)

