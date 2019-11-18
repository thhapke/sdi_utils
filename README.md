# genjson
Generates the **SAP Data Intelligence (SAP DI)** solution files that enables to locally code new custom python operators and prepares for uploading to an **SAP DI** instance as a solution. 

A specific folder structure is required in order use the *gensolution* that complies to a general *github* repository setup. This can be setup with ```gensolution --project```.

In addition the operator script needs to provide additional information that is used to create the operator description files *operator.json* and *configSchema.json*. 

## Required Folder Structure

* project
	* src: containing the source code of the operator packages
		* package1
			* \[operator\] - operator is placeholder for the name of the operator
				* \[operator\].py 
			* package2
				* ... 	 
	* solution: containing the solution folders that corresponds to the **SAP DI** python subengine folder structure. The structure is generated if not existing. 
		* operators 
			* package_version.zip (solution for uploading to a **SAP DI** instance
			* package_version 
				* manifest.json 	 
				* content 
					* files 
						* ...



## Usage
The genjson-script needs to be exectuted at the root of the project folder (Exception when using ```gensolution --project``). 

```
usage: gensolution.py [-h] [--project PROJECT] [--version VERSION] [--debug]
                      [--force]

Generate SAP Data Intelligence solution for local operator code

optional arguments:
  -h, --help         show this help message and exit
  --project PROJECT  Creates new project with folder structure for locally
                     programming operators
  --version VERSION  version format <num.num.num>
  --debug            for debug-level information
  --force            removes subdirectories from <solution/operators>

```
  

## Operator Template

The following code is reduced to the bare minimum you need for local (offline) development of operators. 

```

def process(msg):

    # start custom code
    
    # example of how to use config parameters
    filename = api.config.filename

    # end custom code

    # return message
    return  api.Message(attributes = {'filename':api.config.filename}, body=data)

### definitions for local development to test 
### - will not be read when called on SAP Data Intelligence

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
            msg = process(msg)
            return msg

# list input and output ports with specified types for creating operator.json
inports = [{"name":"input","type":"message"}]
outports = [{"name":"output","type":"message"}]

def interface(msg):
    msg, info = process(msg)
    api.send(outports[0]["name"], msg)

# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback(inports[0]["name"], interface)

```

