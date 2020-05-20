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

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':'', 'pandas':''}
            version = "0.0.1"
            operator_description = "dict to table"
            operator_name = 'dict_table'
            operator_description_long = "Converts dict to table (2-dim. array)"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg) :
    att_dict = msg.attributes

    att_dict['operator'] = 'dict_table'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode,stream_output=True)
    logger.info("Process started. Logging level: {}".format(logger.level))

    logger.debug('Attributes: {}'.format(str(msg.attributes)))
    df = pd.DataFrame(msg.body)

    for c in df.columns:
        if not 'table' in att_dict :
            att_dict['table'] = {'columns':[]}
        att_dict['table']['columns'].append({'name':c})

    table = df.values.tolist()


    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict,body=table))


inports = [{'name': 'dict', 'type': 'message',"description":"Input dict"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'table', 'type': 'message.table',"description":"Output table"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    api.set_config(config)

    data = [{'name': 'Anna', 'age': 25, 'born': '1995-03-14'}, {'name': 'Berta', 'age': 31, 'born': '1989-08-14'},
            {'name': 'Cecilia', 'age': 41, 'born': '1979-08-14'}]
    msg = api.Message(attributes={'format': 'json'}, body=data)
    process(msg)


if __name__ == '__main__':
    test_operator()
    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        print('Solution name: {}'.format(solution_name))
        print('Current directory: {}'.format(os.getcwd()))
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_0.0.1',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


