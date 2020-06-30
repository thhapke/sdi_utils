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
            operator_name = 'repl_prep_write'
            operator_description = "Prepare Write"

            operator_description_long = "Prepare \'write\' of replicated data"
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            drop_replication_cols = False
            config_params['drop_replication_cols'] = {'title': 'Drop Replication columns',
                                           'description': 'Drop Replication columns.',
                                           'type': 'boolean'}


def process(msg):

    att_dict = msg.attributes
    att_dict['operator'] = 'repl_prep_write'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(att_dict)))

    df = msg.body
    logger.debug('Dataframe shape: {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Dataframe columns: {}'.format(df.columns) )

    if df.shape[0] == 0:
        logger.warning('Empty Package')
    else:
        package_id_list = df['PACKAGEID'].unique().tolist()
        if len(package_id_list) > 1:
            logger.error('More than \'1\' package id.')
            raise ValueError('More than \'1\' package id: {}'.format(package_id_list))
        att_dict['packageid'] = package_id_list[0]

        if api.config.drop_replication_cols:
            df = df.drop(columns=['PACKAGEID', 'STATUS', 'UPDATED'])

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict,body=df))


inports = [{'name': 'data', 'type': 'message.DataFrame', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Prep data"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():

    df = pd.DataFrame(
        {'icol': [1, 2, 3, 4, 5], 'UPDATED': ['2020-01-01', '2020-02-01', '2020-01-31', '2020-01-28', '2020-04-12'],
         'PACKAGEID': [1001, 1001, 1001, 1001, 1001]  ,  'STATUS': ['New', 'New', 'New', 'New', 'New']  ,            \
         'col3': [100.0, 200.2, 300.4, 400, 500], 'names': ['Anna', 'Berta', 'Berta', 'Claire', 'Dora'], \
         'bool': [True, False, False, True, True]})

    msg = api.Message(attributes={'data':'replication'},body=df)
    process(msg)

    print(api.queue[0].body)


if __name__ == '__main__':
    #test_operator()
    if True:
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

