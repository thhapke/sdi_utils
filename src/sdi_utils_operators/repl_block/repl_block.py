import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
import random
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
            operator_name = 'repl_block'
            operator_description = "Repl. Block"

            operator_description_long = "Update replication table status to done."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    att = {} # do not copy the msg.attributes, because only ref not value is been used
    att['operator'] = 'repl_block'
    att['table'] = msg.attributes['table']
    att['base_table'] = msg.attributes['base_table']
    att['latency'] = msg.attributes['latency']
    att['data_outcome'] = msg.attributes['data_outcome']
    #att['pid'] = msg.attributes['pid']
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(dict)))

    logger.info('Replication table from attributes: {}'.format(att['table']))

    pid = int(random.random() * 100000000)
    att['pid'] = pid

    update_sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\', '\
                 '\"DIREPL_UPDATED\" =  CURRENT_UTCTIMESTAMP WHERE ' \
                 '\"DIREPL_PACKAGEID\" = (SELECT min(\"DIREPL_PACKAGEID\") ' \
                 'FROM {table} WHERE \"DIREPL_STATUS\" = \'W\')'.format(table=att['table'], pid = pid)

    logger.info('Update statement: {}'.format(update_sql))
    att['update_sql'] = update_sql

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))

    api.send(outports[1]['name'], update_sql)
    api.send(outports[2]['name'], api.Message(attributes=att,body=update_sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'string', "description": "sql statement"},
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'packageid':4711,'table':'repl_table','base_table':'repl_table','latency':30,'data_outcome':True},body='')
    process(msg)

    for st in api.queue :
        print(st)


if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

