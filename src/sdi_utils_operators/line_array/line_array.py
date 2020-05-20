import io
import json
import os
import subprocess

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
            operator_name = 'line_array'
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

    logger.debug('Attributes: {}'.format(str(msg.attributes)))

    lines_io = io.StringIO(msg.body.decode('utf-8'))
    lines = lines_io.read().splitlines()
    lines = list(filter(None, lines))

    all_lines.extend(lines)

    if api.config.lexicographical :
        all_lines.sort()

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    if 'message.lastBatch' in msg.attributes :
        if msg.attributes['message.lastBatch'] or api.config.collect == False:
            msg = api.Message(attributes=att_dict, body=all_lines)
            api.send(outports[1]['name'], msg)
    else :
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


inports = [{'name': 'stream', 'type': 'message.file',"description":"Input csv byte or string"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.list',"description":"Output data as list"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    config.collect = False
    config.lexicographical = True

    fname = '/Users/Shared/data/onlinemedia/repository/blacklist.txt'

    fbase = fname.split('.')[0]
    attributes = {'format': 'csv', "storage.filename": fbase, 'message.lastBatch': True, \
                  'storage.fileIndex': 0, 'storage.fileCount': 1,'process_list':[]}
    csvstream = open(fname, mode='rb').read()
    msg = api.Message(attributes=attributes, body=csvstream)
    api.call(config=config, csvstream = msg)


if __name__ == '__main__':
    #test_operator()
    if True :
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
