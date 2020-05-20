import io
import json
import os
import re
import csv
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
            operator_name = 'csv_dict'
            operator_description = "csv stream to dict"
            operator_description_long = "Converts csv stream to dict"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data', 'description': 'Collect data before sending it to the output port',
                                           'type': 'boolean'}
            separator = ';'
            config_params['separator'] = {'title': 'Separator', 'description': 'Separator',
                                           'type': 'string'}

            column_dict = 'key_col_header:value_col_header'
            config_params['column_dict'] = {'title': 'Column dictionary', 'description': 'Column dictionary with first \
with first column as key and second column as value','type': 'string'}

result_dicts = list()

def process(msg):
    att_dict = msg.attributes

    global result_dicts

    att_dict['operator'] = 'csv_dict'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    logger.debug('Attributes: {}'.format(str(att_dict)))

    str_decode = msg.body.decode('utf-8')
    csv_io = io.StringIO(str_decode)
    dictreader = csv.DictReader(csv_io, delimiter=api.config.separator)

    col_dict = tfp.read_dict(api.config.column_dict)
    if col_dict :
        if api.config.collect :
            error_msg = 'Error: parameter <collect=True> and column dict are not implemented. Choose either-or.'
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.info("Create dict with first column as key and second as value")
        for key_col,val_col in col_dict.items() :
            cdict = {}
            for row in dictreader:
                for header, value in row.items():
                    try:
                        cdict[header].append(value)
                    except KeyError:
                        cdict[header] = [value]
            result_dicts.extend(cdict)
    else:
        logger.info("Create list of dictionaries for each row")
        dict_list = [x for x in dictreader]
        result_dicts.extend(dict_list)

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    if msg.attributes['message.lastBatch'] or api.config.collect == False:
        msg = api.Message(attributes=att_dict, body=result_dicts)
        api.send(outports[1]['name'], msg)

    if api.config.debug_mode:
        for i, dk in enumerate(result_dicts):
            logger.debug(dk)
            if i > 100:
                break

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'stream', 'type': 'message.file',"description":"Input file message"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.dicts',"description":"Output data as list of dictionaries"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    config.collect = False
    config.separator = ','
    config.column_dict = 'index:keyword'

    fname = '/Users/Shared/data/onlinemedia/repository/lexicon.csv'

    fbase = fname.split('.')[0]
    attributes = {'format': 'csv', "storage.filename": fbase, 'message.lastBatch': True, \
                  'storage.fileIndex': 0, 'storage.fileCount': 1,'process_list':[]}
    csvstream = open(fname, mode='rb').read()
    msg = api.Message(attributes=attributes, body=csvstream)
    api.call(config=config, csvstream = msg)


if __name__ == '__main__':
    test_operator()
    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version

        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_0.0.1',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])
        
