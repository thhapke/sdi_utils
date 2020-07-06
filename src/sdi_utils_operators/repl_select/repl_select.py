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
            if port == outports[2]['name']:
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils': ''}
            operator_name = 'repl_select'
            operator_description = "Repl. Select"

            operator_description_long = "Creates SELECT SQL-statement for replication."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



def process(msg):

    att = {}
    att['operator'] = 'repl_select'
    att['table'] = msg.attributes['table']
    att['base_table'] = msg.attributes['base_table']
    att['latency'] = msg.attributes['latency']
    att['data_outcome'] = msg.attributes['data_outcome']
    att['pid'] = msg.attributes['pid']
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))

    select_sql = 'SELECT * FROM {table} WHERE \"DIREPL_STATUS\" = \'B\' AND  \"DIREPL_PID\" = \'{pid}\' '.\
        format(table=att['table'],pid= att['pid'])
    att['select_sql'] = select_sql
    msg = api.Message(attributes=att,body = select_sql)

    logger.info('SELECT statement: {}'.format(select_sql))
    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[1]['name'], select_sql)
    api.send(outports[2]['name'], msg)

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'trigger', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'string', "description": "sql statement"},
            {'name': 'msg', 'type': 'message', "description": "message with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    msg = api.Message(attributes={'pid': 123123213, 'table':'REPL_TABLE','base_table':'REPL_TABLE','latency':30,'data_outcome':True},body='')
    process(msg)

    for m in api.queue:
        print('Attributes: \n{}'.format(m.attributes))
        print('Body: \n{}'.format(m.body))


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

