

def process(msg):

    # core process
    # start custom code
    filename = api.config.filename
    with open(filename, 'r') as fin :
        data = fin.read()
        fin.close()
    # end custom code

    # return message
    return  api.Message(attributes = {'filename':api.config.filename}, body=data), log_stream.getvalue()

'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

try:
    api
except NameError:
    class api:
        # default input - only for testing
        def get_default_input():
            body = '0'
            attributes = {'format': 'csv', "storage.filename": "test_file.txt"}
            return api.Message(attributes=attributes, body=body)

        # config parameter are required for testing and generating configSchema.json
        class config:
            # operator infos necessary for solution import
            tags = {'python36': ''}  # tags that helps to select the appropriate container
            operator_description = 'Read File from Container'
            version = "0.0.2"  # for creating the manifest.json

            # operator parameter for config the operator
            config_params=dict()
            filename = './data/test_file.txt'
            #info for necessary for solution import
            config_params['filename'] = {'title': 'Filename', 'description':'Filename (path)', 'type':'string'}

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg,api.Message) :
                print('--- Attributes: --- ')
                print(msg.attributes)
                print('--- Body: --- ')
                print(msg.body)

        def set_port_callback(port, callback):
            print("Port: ",port)
            msg = api.get_default_input()
            callback(msg)

        def call(msg,config):
            api.config = config
            msg, info = process(msg)
            return msg, info

# list input and output ports with specified types for creating operator.json
inports = [{"name":"input","type":"message"}]
outports = [{"name":"output","type":"message"}]

def interface(msg):
    msg, info = process(msg)
    api.send(outports[0]["name"], msg)

# Triggers the request for every message
api.set_port_callback(inports[0]["name"], interface)
