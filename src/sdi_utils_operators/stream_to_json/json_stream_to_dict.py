import io
import json
import os
import re
import html

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

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
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', msg.attributes['storage.filename']), \
                          mode='w') as f:
                    f.write(json.dumps(msg.body,ensure_ascii=False))
            else :
                print('{}: {}'.format(port,msg))
    
        def call(config,jstream):
            api.config = config
            return process(jstream)
            
        def set_port_callback(port, callback) :
            pass
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'pandas': '','sdi_utils':''}
            version = "0.0.1"
            operator_description = "JSON stream to dict"
            operator_description_long = "Converts json stream to dict"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data', 'description': 'Collect data before sending it to the output port',
                                           'type': 'boolean'}

result_jdict = list()

def process(msg) :
    att_dict = msg.attributes

    global result_jdict

    att_dict['operator'] = 'json_stream_to_dict'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    logger.info('Filename: {} index: {}  count: {}  endofSeq: {}'.format(msg.attributes["storage.filename"], \
                                                                         msg.attributes["storage.fileIndex"], \
                                                                         msg.attributes["storage.fileCount"], \
                                                                         msg.attributes["storage.endOfSequence"]))

    if msg.body == None:
        logger.info('Process ended.')
        msg = api.Message(attributes=att_dict, body=result_jdict)
        log = log_stream.getvalue()
        return log, msg
    elif isinstance(msg.body, str):
        json_io = io.StringIO(msg.body)
        logger.debug("Input format: <string>")
    elif isinstance(msg.body, bytes):
        json_io = io.BytesIO(msg.body)
        logger.debug("Input format: <bytes>")
    elif isinstance(msg.body, io.BytesIO):
        logger.debug("Input format: <io.Bytes>")
        json_io = msg.body
    else:
        raise TypeError('Message body has unsupported type' + str(type(msg.body)))

    jdict = json.load(json_io)
    if not isinstance(jdict,list) :
        jdict = [jdict]
    result_jdict.extend(jdict)

    ## Unescape test
    for art in result_jdict :
        print(art['text'])

    progress_str = '<BATCH ENDED><1>'
    if 'storage.fileIndex' in att_dict and 'storage.fileCount' in att_dict and 'storage.endOfSequence' in att_dict:
        if att_dict['storage.fileIndex'] + 1 == att_dict['storage.fileCount']:
            progress_str = '<BATCH ENDED><{}>'.format(att_dict['storage.fileCount'])
        else:
            progress_str = '<BATCH IN-PROCESS><{}/{}>'.format(att_dict['storage.fileIndex'] + 1,
                                                              att_dict['storage.fileCount'])
    logger.debug('Process ended: {}  - {}  '.format(progress_str, time_monitor.elapsed_time()))
    if msg.attributes["storage.endOfSequence"] or api.config.collect == False:
        msg = api.Message(attributes=att_dict,body=result_jdict)
        api.send(outports[1]['name'], msg)

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'stream', 'type': 'message',"description":"Input json byte or string"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.Dictionary',"description":"Output data as dictionary"}]


#api.set_port_callback(inports[0]['name'], process)

def main() :
    config = api.config
    config.debug_mode = True
    config.collect = True

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json', f)]
    for i, fname in enumerate(files_in_dir):
        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        jsonstream = open(os.path.join(in_dir, fname), mode='rb').read()
        msg = api.Message(attributes=attributes, body=jsonstream)
        api.call(config=config, jstream = msg)


if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
