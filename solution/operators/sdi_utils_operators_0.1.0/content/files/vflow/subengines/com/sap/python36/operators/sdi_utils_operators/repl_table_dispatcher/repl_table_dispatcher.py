import io

import os
import pandas as pd
import numpy as np

import subprocess

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
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Repl. Table Dispatcher"
            operator_name = 'repl_table_dispatcher'
            operator_description_long = "Send next table to replication process."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            periodicity = 0
            config_params['periodicity'] = {'title': 'Periodicity (s)',
                                       'description': 'Periodicity (s).',
                                       'type': 'integer'}


repl_tables = list()
pointer = 0

def set_replication_tables(msg) :
    global repl_tables
    repl_tables = msg.body
    process(msg)

def process(msg) :

    global repl_tables
    global pointer

    att_dict = msg.attributes

    att_dict['operator'] = 'repl_table_dispatcher'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    logger.debug('Attributes: {}'.format(str(msg.attributes)))
    time_monitor = tp.progress()

    # case no repl tables provided
    if len(repl_tables) == 0 :
        logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    repl_table = repl_tables[pointer]
    pointer = (pointer + 1) % len(repl_tables)

    att_dict['latency'] = repl_table['LATENCY']
    att_dict['replication_table'] = repl_table['TABLE']
    if '.' in repl_table['TABLE']  :
        att_dict['base_replication_table'] = repl_table['TABLE'].split('.')[1]
    else :
        att_dict['base_replication_table'] = repl_table['TABLE']
    api.send(outports[1]['name'], api.Message(attributes=att_dict,body=repl_table))

    logger.info('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'tables', 'type': 'message',"description":"List of tables"},
           {'name': 'trigger', 'type': 'message',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'table', 'type': 'string',"description":"Table metadata"}]


api.set_port_callback(inports[1]['name'], process)
api.set_port_callback(inports[0]['name'], set_replication_tables)

def test_operator() :

    api.config.debug_mode = True

    data = [{'TABLE':'repl_TABLE1', 'LATENCY':0},{'TABLE':'repl_TABLE2', 'LATENCY':0},{'TABLE':'repl_TABLE3', 'LATENCY':0},
            {'TABLE':'repl_TABLE4', 'LATENCY':0},{'TABLE':'repl_TABLE5', 'LATENCY':0},{'TABLE':'repl_TABLE6', 'LATENCY':0}]

    msg = api.Message(attributes={'tables':'test_tables'}, body=data)
    set_replication_tables(msg)

    trigger = api.Message(attributes={'table':'test'}, body='go')
    for i in range(0,10) :
        process(msg)

