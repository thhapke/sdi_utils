import pandas as pd
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5

def process(msg):

    # test if body refers to a DataFrame type
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start  calculation
    warning = ''

    # due to parametrization in ML Scenario Manager convert strings to integer
    sample_size = int(tfp.read_value(api.config.sample_size))
    if sample_size > df.shape[0] :
        warning = "Sample size larger than number of rows"

    random_state = int(tfp.read_value(api.config.random_state))

    invariant_column = tfp.read_value(api.config.invariant_column)
    if invariant_column and sample_size < df.shape[0] :
        # get the average number of records for each value of invariant
        sc_df = df.groupby(invariant_column)[invariant_column].count()
        sample_size_invariant = int(sample_size / sc_df.mean())
        sample_size_invariant = 1 if sample_size_invariant == 0 else sample_size_invariant  # ensure minimum
        sc_df = sc_df.sample(n=sample_size_invariant, random_state=random_state).to_frame()
        sc_df.rename(columns={invariant_column: 'sum'}, inplace=True)
        # sample the df by merge 2 df
        df = pd.merge(df, sc_df, how='inner', right_index=True, left_on=invariant_column)
        df.drop(columns=['sum'], inplace=True)
    else :
        df = df.sample(n=sample_size, random_state=random_state)

    ###### end  calculation

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

try:
    api
except NameError:
    class api:

        # input data - only used for isolated testing
        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'A', 'B', 'B', 'C'], \
                 'xcol3': ['K', 'L', 'M', 'N', 'O'], 'xcol4': ['a1', 'a1', 'b1', 'b1', 'b1']})
            att = {'format': 'pandas', 'name': 'test'}

            return api.Message(attributes=att, body=df)

        # definition of api.config - variable names should be same as in DI implementation
        class config:
            random_state = "1"  # integer but provided as string
            sample_size = "2"  # integer but provided as string
            invariant_column = 'None'  # string
            #invariant_column = 'xcol2'  # string

        # fake definition of api.Message
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        # fake definition - can be used of asserting test results
        def send(port, msg):
            if isinstance(msg, str):
                print(msg)
                # pass
            else:
                print(msg.body)
            pass


        def set_port_callback(port, callback):
            if isinstance(port, list):
                port = str(port)
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
api.set_port_callback("inDataFrameMsg", interface)
