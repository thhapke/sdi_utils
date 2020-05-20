import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp

import requests
import subprocess
import os
import csv


try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if outports[1]['name'] == port :
                print(msg.attributes)
                with open('/Users/Shared/data/test/rki.csv', 'w') as f:
                    writer = csv.writer(f)


        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils':'', 'requests':''}
            operator_name = 'HTTPdownload'
            operator_description = "HTTP File Download"
            operator_description_long = "HTTP File Download."
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            url = 'None'
            config_params['url'] = {'title': 'URL of file', 'description': 'URL of file to download', 'type': 'string'}


def process() :
    att_dict = {}
    att_dict['operator'] = 'HTTPdownload'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    logger.info("Process started")

    url_file = tfp.read_value(api.config.url)

    r = requests.get(url_file)
    if r.status_code != 200 :
        logger.warning('File not found: {}. \'None\' send to outport.'.format(url_file))
        downloaded_file = None
    else :
        downloaded_file = r.content

    att_dict["file"] = {"connection": "", "path": ""}
    msg = api.Message(attributes=att_dict, body=downloaded_file)
    api.send(outports[1]['name'], msg)
    logger.info('File send to outport: {}'.format(url_file))
    logger.info("Process ended")
    api.send(outports[0]['name'],log_stream.getvalue())

inports = []
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"csv","type":"message.file","description":"url file"}]

#api.add_generator(process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    config.url = "https://opendata.arcgis.com/datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv"
    api.set_config(config)
    process()



if __name__ == '__main__':
    #test_operator()
    #gs.gensolution(realpath(__file__), api.config, None, outports)
    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        print('Solution name: {}'.format(solution_name))
        print('Current directory: {}'.format(os.getcwd()))
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_0.0.1',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


        
