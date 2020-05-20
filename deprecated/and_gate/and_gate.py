
try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        class config:
            ## Meta data
            config_params = dict()
            tags = {}
            version = "0.0.1"
            operator_description = "AND Gate"
            operator_description_long = "Triggers when input on both input ports."
            add_readme = dict()

inports = [{"name": "input1", "type": "any", "description": "Trigger, content not used"}, \
           {"name": "input2", "type": "any", "description": "Trigger, content not used"}]
outports = [{'name': 'trigger', 'type': 'string', "description": "Trigger next process step"}]


def trigger(msg1,msg2):
    api.send(outports[0]['name'], "Get on")


#api.set_port_callback([inports[0]['name'],inports[1]['name']], trigger)



