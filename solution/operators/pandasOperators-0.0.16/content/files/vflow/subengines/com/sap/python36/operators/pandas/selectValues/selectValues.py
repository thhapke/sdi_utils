import pandas as pd
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS =5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()

    ######################### Start Calculation

    # save and reset indices
    index_names = df.index.names
    if index_names[0]  :
        df.reset_index(inplace=True)

    # prepare selection for numbers
    if api.config.selection_num and not api.config.selection_num.upper() == 'NONE':

        selection_map =tfp.read_relations(api.config.selection_num)

        for s in selection_map :
            if s[1] == '≤' :
                df = df.loc[df[s[0]] <= s[2]]
            elif s[1] == '<' :
                df = df.loc[df[s[0]] < s[2]]
            elif s[1] == '≥':
                df = df.loc[df[s[0]] >= s[2]]
            elif s[1] == '>':
                df = df.loc[df[s[0]] > s[2]]
            elif s[1] == '=':
                df = df.loc[df[s[0]] == s[2]]
            elif s[1] == '!':
                df = df.loc[df[s[0]] != s[2]]
            else :
                raise ValueError('Unknown relation: '+str(s))
    att_dict['config']['selection_num'] = api.config.selection_num


    if api.config.selection_list and not api.config.selection_list.upper() == 'NONE':
        value_list_dict = tfp.read_dict_of_list(api.config.selection_list)
        for key, vl in value_list_dict.items() :
            df = df.loc[df[key].isin(vl)]
    att_dict['config']['selection_list'] = api.config.selection_list

    # set  index again
    if index_names[0]  :
        att_dict['indices'] = index_names
        df.set_index(keys = index_names,inplace=True)

    if df.empty :
        raise ValueError('DataFrame is empty')
    ######################### End Calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]
    if 'id' in prev_att.keys() :
        att_dict['id'] = prev_att['id'] + '; ' + att_dict['operator'] + ': ' + str(id(df))
    else :
        att_dict['id'] = att_dict['operator'] + ': ' + str(id(df))

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''
try:
    api
except NameError:
    class api:

        def get_default_input() :
            df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})
            attributes = {'format': 'csv','name':'DF_name'}
            return api.Message(attributes=attributes,body=df)

        class config:
            selection_num = 'icol <= 5'  # operators comparisons: <,>,=,!=
            selection_list = "'col 2' : 1, 3"  # value lists

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(100))
            pass

        def set_port_callback(port, callback):
            msg = api.get_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config,input):
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


