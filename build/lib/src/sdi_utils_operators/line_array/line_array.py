import io
import json
import os
import re
import csv
import codecs

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
            if port == outports[1]['name'] :
                print(msg.attributes)
                for i,dk in enumerate(msg.body) :
                    print(dk)
                    if i > 100 :
                        break
                print(msg.body)
            else :
                print('{}: {}'.format(port,msg))
    
        def call(config,csvstream):
            api.config = config
            return process(csvstream)
            
        def set_port_callback(port, callback) :
            pass
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_description = "Line to array"
            operator_description_long = "Converts lines to array"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data', 'description': 'Collect data before sending it to the output port',
                                           'type': 'boolean'}

            lexicographical = False
            config_params['lexicographical'] = {'title': 'Lexicographical order', 'description': 'Order array according to lexicographical order ',
                                           'type': 'boolean'}


all_lines = list()

def process(msg):
    att_dict = msg.attributes

    global all_lines

    att_dict['operator'] = 'line_array'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    logger.info('Filename: {} index: {}  count: {}  endofSeq: {}'.format(msg.attributes["storage.filename"], \
                                                                         msg.attributes["storage.fileIndex"], \
                                                                         msg.attributes["storage.fileCount"], \
                                                                         msg.attributes["storage.endOfSequence"]))

    lines_io = io.StringIO(msg.body.decode('utf-8'))
    lines = lines_io.read().splitlines()
    lines = list(filter(None, lines))

    all_lines.extend(lines)

    if api.config.lexicographical :
        all_lines.sort()

    att_dict['batch.index'] = att_dict['storage.fileIndex']
    att_dict['batch.number'] = att_dict['storage.fileCount']
    progress_str = '<BATCH ENDED><1>'
    if 'storage.fileIndex' in att_dict and 'storage.fileCount' in att_dict and 'storage.endOfSequence' in att_dict:
        if att_dict['storage.fileIndex'] + 1 == att_dict['storage.fileCount']:
            progress_str = '<BATCH ENDED><{}>'.format(att_dict['storage.fileCount'])
        else:
            progress_str = '<BATCH IN-PROCESS><{}/{}>'.format(att_dict['storage.fileIndex'] + 1,
                                                              att_dict['storage.fileCount'])
    logger.debug('Process ended: {}  - {}  '.format(progress_str, time_monitor.elapsed_time()))
    if msg.attributes["storage.endOfSequence"] or api.config.collect == False:
        att_dict['batch.last'] = True
        msg = api.Message(attributes=att_dict, body=all_lines)
        api.send(outports[1]['name'], msg)

    if api.config.debug_mode:
        example_lines = 100
        logger.debug('Example lines #{}'.format(example_lines))
        for i, dk in enumerate(all_lines):
            logger.debug(dk)
            if i > example_lines:
                break

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'stream', 'type': 'message',"description":"Input csv byte or string"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.list',"description":"Output data as list"}]


#api.set_port_callback(inports[0]['name'], process)

def main() :
    config = api.config
    config.debug_mode = True
    config.collect = False
    config.lexicographical = True

    fname = '/Users/Shared/data/onlinemedia/repository/disregards.txt'

    fbase = fname.split('.')[0]
    attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': True, \
                  'storage.fileIndex': 0, 'storage.fileCount': 1,'process_list':[]}
    csvstream = open(fname, mode='rb').read()
    msg = api.Message(attributes=attributes, body=csvstream)
    api.call(config=config, csvstream = msg)


if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
