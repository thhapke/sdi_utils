
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
            print('Sending msg: {}'.format(msg.body))

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
            sleep = 5
            config_params['sleep'] = {'title': 'Sleep', 'description': \
                'Time before starting next processing step', 'type': 'integer'}


inports = [{"name": "limit", "type": "integer", "description": "Limit"},\
           {"name": "data", "type": "message", "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"},\
            {"name": "data", "type": "message", "description": "Output of unchanged last input data"}]


counter = 0
def call_on_message(msg):
    op_name = 'Gate'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(op_name, loglevel='DEBUG')
    else:
        logger, log_stream = slog.set_logging(op_name, loglevel='INFO')
    global counter
    counter += 1
    if counter >= api.config.limit :
        time.sleep(api.config.sleep)
        api.send(outports[0],msg)
        logger.info('Counter greater than limit: {} >= {}'.format(counter, api.config.limit))
    else :
        logger.info('No message send. Counter less than limit: {} < {}'.format(counter, api.config.limit))

def call_on_limit(new_limit) :

    api.config.limit = new_limit

#api.set_port_callback(inports[0]['name'], call_on_limit)
#api.set_port_callback(inports[1]['name'], call_on_message)

def main():

    api.config.sleep = 0
    api.config.limit = 5
    for i in range(0,10) :
        msg = api.Message(attributes={'msg': 'test'}, body=i)
        call_on_message(msg)
        if i == 6 :
            call_on_limit(9)


if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)

