import time
import re
import subprocess
import os


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
            if port == outports[2]['name'] :
                print(msg.attributes)
                print(msg.body)


        def set_config(config):
            api.config = config
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'filter_date'
            operator_description = "Filter date string"
            operator_description_long = "Filters the names with today substring."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            start_date = 'None'
            config_params['start_date'] = {'title': 'Start Date',
                                           'description': 'Start date of ',
                                           'type': 'boolean'}

            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

count = 0

def process(msg):
    global count
    att_dict = msg.attributes

    att_dict['operator'] = 'filter_today'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()
    today_str = time.strftime("%Y-%m-%d")
    logger.debug("Today string: {}".format(today_str))

    today_str = [today_str]
    #today_str.extend(['2020-04-02'])

    filename = msg.body

    logger.debug('Test Filename: {}'.format(filename))
    for day_str in today_str :
        if re.search(day_str,filename)  :
            count += 1
            logger.info('Filename with day str {} found: {} ({}/{}/{})'.format(day_str,filename,count,att_dict['storage.fileIndex'],\
                                                                              att_dict['storage.fileCount']))
            api.send(outports[2]['name'],api.Message(attributes=att_dict, body = filename))

    if 'storage.endOfSequence' in att_dict and att_dict['storage.endOfSequence'] == True:
        logger.info('Last File send: {} ({}/{}/{})'.format(filename,count,att_dict['storage.fileIndex'],\
                                                                          att_dict['storage.fileCount']))
        api.send(outports[1]['name'], count)

    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'input', 'type': 'message',"description":"List of filenames"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"},\
            {'name': 'numfiles', 'type': 'int64',"description":"Number of files"},\
            {'name': 'output', 'type': 'message',"description":"Filename"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    api.set_config(config)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json', f)]
    for i, fname in enumerate(files_in_dir):
        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        msg = api.Message(attributes=attributes, body=fname)
        process(msg)


if __name__ == '__main__':
    test_operator()

    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version

        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_0.0.1',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])
