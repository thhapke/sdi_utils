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
            operator_name = 'repl_gen_test_table'
            operator_description = "Generate Test Table"

            operator_description_long = "Generates Test Table."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            off_set = 1
            config_params['off_set'] = {'title': 'Offset of the number column',
                                           'description': 'Offset of the number column',
                                           'type': 'integer'}

            num_rows = 100
            config_params['num_rows'] = {'title': 'Number of table rows',
                                           'description': 'Number of table rows',
                                           'type': 'integer'}

            package_size = 5
            config_params['package_size'] = {'title': 'Package size',
                                           'description': 'Package size',
                                           'type': 'integer'}


def process(msg):
    att_dict = {'table': 'Gen_test', 'offset': api.config.off_set, 'num_rows': api.config.num_rows}
    att_dict['operator'] = 'repl_gen_test_table'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(att_dict)))

    col1 = np.arange(api.config.offset, api.config.offset + api.config.num_rows)
    df = pd.DataFrame(col1, columns=['INT_NUM']).reset_index()
    df.rename(columns={'index': 'INDEX'}, inplace=True)
    dt = datetime.now(timezone.utc)
    df['DIREPL_UPDATED'] = dt.strftime("%Y-%m-%d %H:%M:%S")
    df['DIREPL_PID'] = 0
    df['DIREPL_STATUS'] = 'W'
    df['DIREPL_PACKAGEID'] = 0

    # packageid creation
    #packageid_start = int(random.random() * 10000)
    packageid_start = 0
    for i, start in enumerate(range(0, df.shape[0], api.config.package_size)):
        df.DIREPL_PACKAGEID.iloc[start:start + api.config.package_size] = packageid_start + i

    csv = df.to_csv(sep=',', index=False)

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=csv))

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

