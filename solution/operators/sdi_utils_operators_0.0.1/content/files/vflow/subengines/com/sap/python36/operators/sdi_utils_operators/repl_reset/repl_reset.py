import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
from datetime import datetime, timezone
import time

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
            operator_name = 'repl_reset'
            operator_description = "Repl. Reset"

            operator_description_long = "Update replication table status reset."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    att_dict = msg.attributes
    att_dict['operator'] = 'repl_reset'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(att_dict)))

    data = msg.body
    repl_table = data['TABLE']
    latency = data['LATENCY']
    att_dict['replication_table'] = repl_table
    att_dict['latency'] = latency

    update_sql = 'UPDATE {table} SET \"STATUS\" = \'W\' WHERE  \"STATUS\" = \'B\' '\
                 'AND \"UPDATED\" < ADD_SECONDS(CURRENT_UTCTIMESTAMP,-{latency}) '.format(table=repl_table,latency=latency)

    logger.info('Update statement: {}'.format(update_sql))
    att_dict['update_sql'] = update_sql

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], update_sql)
    api.send(outports[2]['name'], api.Message(attributes=att_dict,body=update_sql))


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'string', "description": "sql statement"},
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'table':'repl_table'},body={'TABLE':'repl_table','LATENCY':20})
    process(msg)

    for st in api.queue :
        print(st)


