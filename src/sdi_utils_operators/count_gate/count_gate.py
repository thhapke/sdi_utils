import time

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
            if isinstance(msg, api.Message):
                print('{}: {}'.format(port, msg.body))
            else:
                print('{}: {}'.format(port, msg))

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'pandas': '', 'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "Gate"
            operator_description_long = "Gate that counts incoming messages and sends out message with latest \
message when gate limit has been reached."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            limit = 0
            config_params['limit'] = {'title': 'Limit', 'description': \
                'Limit after which the message is send', 'type': 'integer'}
            sleep = 0
            config_params['sleep'] = {'title': 'Sleep', 'description': \
                'Time before starting next processing step', 'type': 'integer'}

            attribute = None
            config_params['attribute'] = {'title': 'Attribute', 'description': \
                'Attribute that keeps a number used as limit, e.g. storage.fileCount', 'type': 'string'}

inports = [{"name": "limit", "type": "int64", "description": "Limit"}, \
            {"name": "attributes", "type": "message", "description": "Message with attributes to used"},\
           {"name": "data", "type": "message.*", "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'trigger', 'type': 'string', "description": "Trigger next process step"},
            {"name": "data", "type": "message", "description": "Output of unchanged last input data"}]

counter = 0
logger, log_stream = slog.set_logging('Gate', loglevel=api.config.debug_mode)

def call_on_message(msg):
    global counter
    logger, log_stream = slog.set_logging('Gate', loglevel=api.config.debug_mode)
    counter += 1

    if ('batch' in msg.attributes and msg.attributes['batch']['last'] == True) or \
        ('message.lastBatch' in msg.attributes and msg.attributes['message.lastBatch'] == True)  :
        api.send(outports[2]['name'], msg)
        api.send(outports[1]['name'], '1')
        logger.info('Last batch in attributes')
    elif counter >= api.config.limit:
        time.sleep(api.config.sleep)
        api.send(outports[2]['name'], msg)
        api.send(outports[1]['name'], '1')
        logger.info('Counter greater or equal than limit: {} >= {}'.format(counter, api.config.limit))
        # api.send(outports[0]['name'],'Message send. Counter greater or equal than limit: {} >= {}'.format(counter, api.config.limit))
    else:
        logger.info('No message send. Counter less than limit: {} < {}'.format(counter, api.config.limit))
        # api.send(outports[0]['name'],'No message send. Counter less than limit: {} < {}'.format(counter, api.config.limit))

    api.send(outports[0]['name'], log_stream.getvalue())



def call_on_limit(new_limit):
    logger, log_stream = slog.set_logging('Gate', loglevel=api.config.debug_mode)
    old_limit = api.config.limit
    api.config.limit = new_limit
    logger.info('Set new limit to: {} -> {}'.format(old_limit, api.config.limit))
    api.send(outports[0]['name'], log_stream.getvalue())


def call_on_attribute(msg):
    logger, log_stream = slog.set_logging('Gate', loglevel=api.config.debug_mode)
    old_limit = api.config.limit
    limit_attribute = tfp.read_value(api.config.attribute)
    if not limit_attribute :
        raise ValueError('Attribute has not been set in gate parameters.')
    api.config.limit = msg.attributes[limit_attribute]
    logger.info('Set new limit to: {} -> {}'.format(old_limit, api.config.limit))
    api.send(outports[0]['name'], log_stream.getvalue())


#api.set_port_callback(inports[0]['name'], call_on_limit)
#api.set_port_callback(inports[1]['name'], call_on_attribute)
#api.set_port_callback(inports[2]['name'], call_on_message)



def main():
    api.config.sleep = 0
    api.config.limit = 5
    api.config.attribute = 'storage.fileCount'

    for i in range(0, 0):
        msg = api.Message(attributes={'msg': 'test'}, body=i)
        call_on_message(msg)
        if i == 6:
            call_on_limit(9)

    trigger_msg = api.Message(attributes={'storage.fileCount': 3},body='msgbody')
    call_on_attribute(trigger_msg)
    for i in range(0, 10):
        msg = api.Message(attributes={'msg': 'test'}, body=i)
        call_on_message(msg)


if __name__ == '__main__':
    main()