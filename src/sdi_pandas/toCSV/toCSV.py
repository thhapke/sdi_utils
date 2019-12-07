import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog

try:
    api
except NameError:
    class api:
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
    
        def call(config,msg):
            api.config = config
            return process(msg)
            
        def set_port_callback(port, callback) :
            default_msg = api.Message(attributes={'name':'doit'},body = 'message')
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_pandas': ''}
            operator_description = "toCSV"
            write_index = False
            config_params['write_index'] = {'title': 'Write Index', 'description': 'Write index or ignore it', 'type': 'boolean'}
            separator = ';'
            config_params['separator'] = {'title': 'Separator', 'description': 'Separator', 'type': 'string'}
            reset_index = False
            config_params['reset_index'] = {'title': 'Reset Index', 'description': 'Reset index or indices', 'type': 'boolean'}


def process(msg) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    log = None
    data_str = None
    # end custom process definition

    log = log_stream.getvalue()
    return log, data_str


inports = [{'name': 'inDataFrameMsg', 'type': 'message.DataFrame'}]
outports = [{'name': 'Info', 'type': 'string'}, {'name': 'outCSVMsg', 'type': 'string'}]

def call_on_input(msg) :
    log, data_str = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], data_str)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
