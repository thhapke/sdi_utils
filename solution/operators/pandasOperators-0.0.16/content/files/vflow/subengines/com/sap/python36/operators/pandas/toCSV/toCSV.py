import pandas as pd
import json

EXAMPLE_ROWS = 5


def process(msg):
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['config']['separator'] = api.config.separator
    att_dict['config']['write_index'] = api.config.write_index

    msg.body = None
    try :
        df = msg.body
        df = df.reset_index()
        body = df.to_csv(sep=api.config.separator, index=False)
    except AttributeError as err :
        warning = 'Attribute Error msg.body: ' + str(err)
        att_dict['operator'] = 'toCSVDataFrame'  # name of operator
        att_dict['name'] = msg.attributes['name']
        att_dict['warning'] = msg.attributes['warning'] + '; ' + att_dict['operator'] + ':' + warning

        return api.Message(attributes=att_dict, body=None)

    #####################
    #  final infos to attributes and info message
    #####################
    att_dict['operator'] = 'toCSVDataFrame'  # name of operator
    att_dict['name'] = msg.attributes['name']

    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return api.Message(attributes=att_dict, body=body)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''


class test:
    SIMPLE = 0
    ORDER_HEADERS = 1


test_scenario = test.SIMPLE

try:
    api
except NameError:
    class api:
        # input data
        def set_test(test_scenario):
            if test_scenario == test.ORDER_HEADERS:
                df = pd.read_csv("/Users/d051079/OneDrive - SAP SE/Datahub-Dev/data/order_headers.csv", sep=';')
                df.set_index(keys='order_id', inplace=True)
            else:  # SIMPLE
                df = pd.DataFrame(
                    {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'B', 'C', 'D', 'E'], 'xcol3': ['K', 'L', 'M', 'N', 'O']})
                df.set_index(keys='icol', inplace=True)

            # input data
            att_dict = {'format': 'pandas', 'name': 'isolated_test','warning':''}

            return api.Message(attributes=att_dict, body=df)

        def set_config(test_scenario):
            if test_scenario == test.ORDER_HEADERS:
                api.config.write_index = True
                api.config.separator = ';'
            else:  # SIMPLE
                api.config.write_index = True
                api.config.separator = ';'

        class config:
            write_index = False
            separator = ';'

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg, api.Message):
                print(msg.body)
            else:
                print(msg)
            pass

        def set_port_callback(port, callback):
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            msg = api.set_test(test_scenario)
            api.set_config(test_scenario)
            callback(msg)

        # called by 'integrated/pipeline-test simulation
        def test_call(msg):
            print('EXTERNAL CALL of module:' + __name__)
            api.set_config(test_scenario)
            result = process(msg)
            api.send("outDataFrameMsg", result)
            return result


def interface(msg):
    result = process(msg)
    api.send("outCSVMsg", result.body)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback("inDataFrameMsg", interface)

