import time
import subprocess
import os

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                print('Message passed: {} - {}'.format(msg.attributes,msg.body))
            elif port == outports[2]['name']:
                print('Message did not pass: {} - {}'.format(msg.attributes,msg.body))

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.1.0"
            operator_name = 'decision'
            operator_description = "Decision"
            operator_description_long = "Decision gate that channels messages."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            decision_attribute = 'message.lastBatch'
            config_params['decision_attribute'] = {'title': 'Descision Attribute',
                                           'description': 'Decision Attribute',
                                           'type': 'string'}


inports = [{"name": "input", "type": "message.*", "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'True', 'type': 'message.*', "description": "True message"},
            {"name": "False", "type": "message.*", "description": "False message"}]


logger, log_stream = slog.set_logging('Gate', loglevel=api.config.debug_mode)

def process(msg):

    logger, log_stream = slog.set_logging('Gate', loglevel=api.config.debug_mode)

    if api.config.decision_attribute in msg.attributes and msg.attributes[api.config.decision_attribute] == True:
        api.send(outports[1]['name'], msg)
        logger.info('Msg passed: {}'.format(msg.attributes))
        api.send(outports[0]['name'], log_stream.getvalue())
    else :
        api.send(outports[2]['name'], msg)
        logger.info('Msg did not pass: {}'.format(msg.attributes))
        api.send(outports[0]['name'], log_stream.getvalue())


api.set_port_callback(inports[0]['name'], process)


def test_operator():
    #api.config.last_attribute = 'message.last_update'
    process(api.Message(attributes={'message.lastBatch':False},body='0'))
    process(api.Message(attributes={'message.lastBatch':True},body='1'))
    process(api.Message(attributes={'message.last_update': True}, body='2'))


