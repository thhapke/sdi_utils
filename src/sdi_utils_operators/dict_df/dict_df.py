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
                print(msg.attributes)
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':'', 'pandas':''}
            version = "0.1.0"

            operator_description = "dict to df"
            operator_name = 'dict_df'
            operator_description_long = "Converts dict to DataFrame"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            rename = 'None'
            config_params['rename'] = {'title': 'Rename columns',
                                           'description': 'Rename columns by provided map',
                                           'type': 'string'}



def process(msg) :
    att_dict = msg.attributes

    att_dict['operator'] = 'dict_df'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))

    logger.debug('Attributes: {}'.format(str(msg.attributes)))
    if msg.body == None  or len(msg.body) == 0 :
        logger.warning('Empty dictionary received. Empty DataFrame will be send.')

    df = pd.DataFrame(msg.body)

    rename_cols = tfp.read_dict(api.config.rename)
    if rename_cols :
        df.rename(columns = rename_cols,inplace=True)

    logger.debug('DataFrame Shape: {} - {}'.format(df.shape[0],df.shape[1]))

    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict,body=df))


inports = [{'name': 'dict', 'type': 'message',"description":"Input dict"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'table', 'type': 'message.DataFrame',"description":"Output table"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    api.config.debug_mode = True
    #api.config.rename = "name: first_name, age: AGE"


    data = [{'name': 'Anna', 'age': 25, 'born': '1995-03-14'}, {'name': 'Berta', 'age': 31, 'born': '1989-08-14'},
            {'name': 'Cecilia', 'age': 41, 'born': '1979-08-14'}]
    msg = api.Message(attributes={'format': 'json'}, body=data)
    process(msg)


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



