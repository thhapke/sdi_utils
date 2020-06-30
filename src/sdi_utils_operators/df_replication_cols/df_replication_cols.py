import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import os
from datetime import datetime, timezone
import pandas as pd
import random

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
            tags = {'pandas': '', 'sdi_utils': ''}
            operator_name = 'df_replication_cols'
            operator_description = "Adding replciation columns"

            operator_description_long = "Adds replications columns to a table."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            package_size = 1000
            config_params['package_size'] = {'title': 'Package size',
                                       'description': 'Package size',
                                       'type': 'integer'}


def process(msg):
    att_dict = msg.attributes

    att_dict['operator'] = 'df_replication_cols'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(msg.attributes)))

    df = msg.body
    logger.debug('DataFrame: {} - {}'.format(df.shape[0],df.shape[1]))

    #df['DATE'] = pd.to_datetime(df['DATE'], format='%Y-%m-%d', utc=False)

    df['status'] = 'W'
    dt = datetime.now(timezone.utc)
    packageid = dt.strftime("%Y%m%d%H%M%S")
    df['updated'] = dt.strftime("%Y-%m-%d %H:%M:%S")
    #df['packageid'] = df['packageid'].astype('int64')
    df['packageid'] = 0
    att_dict['packageid'] = packageid
    att_dict['updated'] = dt

    # packageid creation
    packageid_start = int(random.random() * 10000)
    package_size = api.config.package_size
    for i, start in enumerate(range(0, df.shape[0], package_size)):
        df.packageid.iloc[start:start + package_size] = packageid_start + i


    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=df))


inports = [{'name': 'table', 'type': 'message.DataFrame', "description": "DataFrame"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'table', 'type': 'message.DataFrame', "description": "DataFrame with replication columns"}]


# api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.package_size = 1
    df = pd.DataFrame(
        {'icol': [1, 2, 3, 4, 5], 'DATE': ['2020-01-01', '2020-02-01', '2020-01-31', '2020-01-28', '2020-04-12'], \
         'col3': [100.0, 200.2, 300.4, 400, 500], 'names': ['Anna', 'Berta', 'Berta', 'Claire', 'Dora'], \
         'bool': [True, False, False, True, True]})

    attributes = {'format': 'csv', 'name': 'DF_name', 'process_list': []}
    msg = api.Message(attributes=attributes, body=df)
    process(msg)

    df_list = [d.body for d in api.queue]
    df = pd.concat(df_list)
    print(df)
    print(df.dtypes)


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

