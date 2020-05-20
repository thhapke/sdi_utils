import sys

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

        def send(port, str):
            print('Send')
            print(str)

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "String Collector"
            operator_description_long = "Converts lines to array"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            threshold_size = 1000
            config_params['threshold_size'] = {'title': 'Threshold size',
                                        'description': 'Threshold by which string is send. ',
                                        'type': 'integer'}

collector_str = ''
logger, log_stream = slog.set_logging('string_collector', loglevel=api.config.debug_mode)

def collect(str):

    global collector_str
    global logger, log_stream

    collector_str += str
    mem = sys.getsizeof(collector_str)
    if mem > api.config.threshold_size * 1024 :
        api.send(outports[1]['name'],collector_str)
        logger.debug('String sent to output due to size: {}kB ({})'.format(mem/1024,api.config.threshold_size))
        api.send(outports[0]['name'],log_stream.getvalue())
        collector_str = ''

def trigger(trig) :
    global collector_str
    logger.debug('String sent to output due to trigger: {:.1f}kB ({})'.format(sys.getsizeof(collector_str)/ 1024, api.config.threshold_size))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'],collector_str)
    collector_str = ''

inports = [{'name': 'string', 'type': 'string', "description": "Input string"},
           {'name': 'trigger', 'type': 'any', "description": "Trigger input"}]
outports = [{'name': 'log', 'type': 'string', "description": "Log strings"},
            {'name': 'string', 'type': 'string', "description": "Output collected string"}]


#api.set_port_callback(inports[0]['name'], collect)
#api.set_port_callback(inports[0]['name'], trigger)

def main():
    config = api.config
    config.debug_mode = True
    config.threshold_size = 1
    api.set_config(config)

    for i in range (0,105) :
        collect('text line: {}\n'.format(i))
    trigger('los')



if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)

