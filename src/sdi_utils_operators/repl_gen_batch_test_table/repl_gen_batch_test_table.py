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
import numpy as np

pd.set_option('mode.chained_assignment',None)

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
            operator_name = 'repl_gen_batch_test_table'
            operator_description = "Generate Batch Test Tables"

            operator_description_long = "Generate Batch Test Tables."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            num_rows = 100
            config_params['num_rows'] = {'title': 'Number of table rows',
                                           'description': 'Number of table rows',
                                           'type': 'integer'}

            package_size = 5
            config_params['package_size'] = {'title': 'Package size',
                                           'description': 'Package size',
                                           'type': 'integer'}

            num_tables = 10
            config_params['num_tables'] = {'title': 'Number of tables',
                                           'description': 'Number of tables.',
                                           'type': 'integer'}

            base_table_name = '"REPLICATION"."TEST_TABLE"'
            config_params['base_table_name'] = {'title': 'Base Table Name',
                                           'description': 'Base Table Name.',
                                           'type': 'string'}


def process(msg):

    att_dict = {}
    att_dict['operator'] = 'repl_gen_batch_test_table'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(att_dict)))

    col1 = np.arange(0, api.config.num_rows)
    df = pd.DataFrame(col1, columns=['INT_NUM']).reset_index()
    df.rename(columns={'index': 'INDEX'}, inplace=True)
    dt = datetime.now(timezone.utc)
    df['DIREPL_UPDATED'] = dt.strftime("%Y-%m-%d %H:%M:%S")
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    df['DIREPL_PACKAGEID'] = 0

    # packageid creation
    #packageid_start = int(random.random() * 10000)

    att_dict['hana.preparedStatement'] = "INSERT INTO {} VALUES".format(api.config.base_table_name+str(0))
    packageid_start = 0
    for i, start in enumerate(range(0, df.shape[0], api.config.package_size)):
        df.DIREPL_PACKAGEID.iloc[start:start + api.config.package_size] = packageid_start + i

    logger.info('Create Table offset: 0')
    csv = df.to_csv(sep=',', index=False)
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=csv))

    for i in range (1,api.config.num_tables) :
        df['INT_NUM'] = df['INT_NUM'] + 1
        logger.info('Create Table offset: {}'.format(i))
        csv = df.to_csv(sep=',', index=False)
        att_dict['hana.preparedStatement'] = "INSERT INTO {} VALUES".format(api.config.base_table_name + str(i))
        api.send(outports[1]['name'], api.Message(attributes=att_dict, body=csv))

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'csv', 'type': 'message', "description": "msg with csv"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.off_set = 2
    api.config.num_rows = 10
    msg = api.Message(attributes={'packageid':4711,'replication_table':'repl_table'},body='')
    process(msg)

    for st in api.queue :
        print(st.body)


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

