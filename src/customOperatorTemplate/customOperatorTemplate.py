
import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.tprogress as tp

import os


try:
    api
except NameError:
    class api:
        class config:

            ## Meta data
            tags = {'python36': ''}  # tags that helps to select the appropriate container
            operator_description = 'Custom Operator Template'
            version = "0.0.1"  # for creating the manifest.json
            config_params = dict()

            ## config paramter
            var1 = 'foo'
            config_params['var1'] = {'title': 'Variable1', 'description': 'Variable 1 for test', 'type': 'string'}
            var2 = 'bar'
            config_params['var2'] = {'title': 'Variable2', 'description': 'Variable 2 for test', 'type': 'string'}

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port,msg) :
            if isinstance(msg,api.Message) :
                print('Port: ', port)
                print('Attributes: ', msg.attributes)
                print('Body: ', str(msg.body))
            else :
                print(str(msg))
            return msg

        def set_port_callback(port, callback) :
            default_msg = api.Message(attributes={'name':'doit'},body = 3)
            callback(default_msg)

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    logger, log_stream = slog.set_logging('DEBUG')
    time_monitor = tp.progress()

    result = ''
    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())
    for i in range (0,msg.body) :
        result += str(i) + ':' + api.config.var1 + ' - ' + api.config.var2 + '    '
    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())
    return api.Message(attributes={'name':'concat','type':'str'},body=result),log_stream.getvalue()

inports = [{"name":"input","type":"message"}]
outports = [{"name":"output","type":"message"},{"name":"log","type":"string"}]

def call_on_input(msg) :
    new_msg, log = process(msg)
    api.send(outports[0]['name'],new_msg)
    api.send(outports[1]['name'],log)

#api.set_port_callback('input', call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.var1 = 'own foo'
    config.var12 = 'own bar'
    test_msg = api.Message(attributes={'name':'test1'},body =4)
    new_msg, log = api.call(config,test_msg)
    print('Attributes: ', new_msg.attributes)
    print('Body: ', str(new_msg.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports)



if __name__ == '__main__':
    main()
