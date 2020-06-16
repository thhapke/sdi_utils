import io
import json
import os
import re
import subprocess

import pandas as pd
import numpy as np

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:

        queue = list()
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]["name"] :
                api.queue.append(msg)

        def set_config(config):
            api.config = config
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_description = "json to df"
            operator_name = 'json_df'
            operator_description_long = "Converts json stream to DataFrame"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



def process(msg) :
    att_dict = msg.attributes

    att_dict['operator'] = 'json_df'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode,stream_output=True)
    logger.info("Process started. Logging level: {}".format(logger.level))

    logger.debug('Attributes: {}'.format(str(msg.attributes)))

    jdict = json.loads(msg.body)
    if not isinstance(jdict,list) :
        jdict = [jdict]

    df = pd.DataFrame(jdict)
    logger.debug('DataFrame created: {} - {}'.format(df.shape[0],df.shape[1]))

    msg = api.Message(attributes=att_dict,body=df)
    api.send(outports[1]['name'], msg)
    logger.info('Msg send to port: {}'.format(outports[1]['name']))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'stream', 'type': 'message.file',"description":"Input json byte or string"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame',"description":"Output DataFrame"}]


api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    config.collect = False
    api.set_config(config)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json', f)]
    for i, fname in enumerate(files_in_dir):
        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        jsonstream = open(os.path.join(in_dir, fname), mode='rb').read()
        msg = api.Message(attributes=attributes, body=jsonstream)
        process(msg)

    out_file = '/Users/Shared/data/test/json_df.csv'
    df_list = [d.body for d in api.queue]
    pd.concat(df_list).to_csv(out_file,index=False)

