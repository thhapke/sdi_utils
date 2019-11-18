

def process(msg):

    # start custom code

    # example of accessing config parameter
    filename = api.config.filename

    # end custom code

    # return message
    return  api.Message(attributes = {'filename':api.config.filename}, body=filename)

### definitions for local development to test
### - will not be read when called by vflow of SAP Data Intelligence
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
            config_params = dict()

            # operator parameter for config the operator and producing configSchema.json
            filename = './data/test_file.txt'
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

        # just takes default
        def set_port_callback(port, callback):
            print("Port: ",port)
            msg = api.get_default_input()
            callback(msg)

        # call function providing input data and config parameter
        def call(msg,config,port,callback):
            api.config = config
            msg = process(msg)
            callback(msg)
            return msg

# list input and output ports with specified types for creating operator.json
inports = [{"name":"input","type":"message"}]
outports = [{"name":"output","type":"message"}]

#def interface(msg):
#    msg = process(msg)
#    api.send(outports[0]["name"], msg)

# Triggers the request for every message - will be un-commented with gensolution or do it manually
api.set_port_callback(inports[0]["name"], interface)

## test standalone
if __name__ == '__main__':
    config = api.config
    config.filename = "./newfile.txt"
    msg = api.Message(attributes={"filename":"new","Suffix":config.filename.split('.')[-1]},body='do it')
    api.call(msg,config)