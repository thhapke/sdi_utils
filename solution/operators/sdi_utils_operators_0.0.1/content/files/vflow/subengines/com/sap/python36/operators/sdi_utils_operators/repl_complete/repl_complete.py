import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
from datetime import datetime, timezone
import pandas as pd

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils': ''}
            operator_name = 'repl_complete'
            operator_description = "Repl. Complete"

            operator_description_long = "Update replication table status to complete."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

def process(msg):

    att = dict()
    att['operator'] = 'repl_complete'
    att['table'] = msg.attributes['table']
    att['latency'] = msg.attributes['latency']
    att['data_outcome'] = msg.attributes['data_outcome']
    att['pid'] = msg.attributes['pid']
    att['packageid'] = msg.attributes['packageid']

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))

    # The constraint of STATUS = 'B' due the case the record was updated in the meanwhile
    update_sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'C\', \"DIREPL_UPDATED\" =  CURRENT_UTCTIMESTAMP WHERE ' \
                 '\"DIREPL_PACKAGEID\" = {packageid} AND \"DIREPL_STATUS\" = \'B\''.format(table=att['table'], packageid = att['packageid'])


    logger.info('Update statement: {}'.format(update_sql))
    att['update_sql'] = update_sql

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[1]['name'], update_sql)
    api.send(outports[2]['name'], api.Message(attributes=att,body=update_sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message.file', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'string', "description": "sql statement"},
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'pid': 123123213, 'table':'REPL_TABLE','base_table':'REPL_TABLE','latency':30,'data_outcome':True,'packageid':1},body='')
    process(msg)

    for st in api.queue :
        print(st)


