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
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "merge attributes"
            operator_name = 'merge_attributes'
            operator_description_long = "Merges the attributes"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



def process(msg1,msg2) :
    att_dict = dict(msg1.attributes)
    msg1.attributes.update(msg2.attributes)
    msg2.attributes.update(att_dict)

    att_dict['operator'] = 'merge_attributes'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.debug('Attributes: {}'.format(str(msg1.attributes)))

    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], msg1)
    api.send(outports[2]['name'], msg2)


inports = [{'name': 'message1', 'type': 'message',"description":"Input message1"},
           {'name': 'message2', 'type': 'message',"description":"Input message2"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'message1', 'type': 'message',"description":"Output message1"},
            {'name': 'message2', 'type': 'message',"description":"Output message2"}]


#api.set_port_callback([inports[0]['name'],inports[1]['name']], process)

def test_operator() :

    api.config.debug_mode = True

    data = [{'name': 'Anna', 'age': 25, 'born': '1995-03-14'}, {'name': 'Berta', 'age': 31, 'born': '1989-08-14'},
            {'name': 'Cecilia', 'age': 41, 'born': '1979-08-14'}]
    msg1 = api.Message(attributes={'format': 'json'}, body=data)
    msg2 = api.Message(attributes={'new':'NEW','more':'MORE'},body='nothing')

    process(msg1,msg2)


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



