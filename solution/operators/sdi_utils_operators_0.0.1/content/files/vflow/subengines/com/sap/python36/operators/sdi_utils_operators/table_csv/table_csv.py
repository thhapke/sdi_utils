import io
import subprocess
import os
import pandas as pd

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port,msg) :
            if port == outports[1]['name'] :
                print('ATTRIBUTES: ')
                print(msg.attributes)#
                print('CSV-String: ')
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'table_csv'
            operator_description = "table to csv"
            operator_description_long = "Converts table to csv stream."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            drop_columns = 'None'
            config_params['drop_columns'] = {'title': 'Drop Columns',
                                           'description': 'List of columns to drop.',
                                           'type': 'string'}


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'table_csv'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started")
    time_monitor = tp.progress()

    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body,columns=header)

    drop_columns = tfp.read_list(api.config.drop_columns)
    if drop_columns :
        df = df.drop(columns = drop_columns)

    csv = df.to_csv(index=False)
    att["file"] =  {"connection": {"configurationType": "Connection Management", "connectionID": "unspecified"}, \
                    "path": "open", "size": 0}

    msg = api.Message(attributes=att,body = csv)
    api.send(outports[1]['name'],msg)

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table',"description":"Input message with table"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'csv', 'type': 'message.file',"description":"Output data as csv"}]


api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True
    api.config.drop_columns = "header2"

    attributes = {"table":{"columns":[{"class":"string","name":"header1","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header2","nullable":True,"size":3,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header3","nullable":True,"size":10,"type":{"hana":"NVARCHAR"}}],
                           "name":"test.table","version":1}}
    table = [ [(j*3 + i) for i in range(0,3)] for j in range (0,5)]
    msg = api.Message(attributes=attributes, body=table)
    print(table)
    process(msg)


