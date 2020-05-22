import time
from datetime import datetime
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
            if port == outports[1]['name'] :
                print('Number of files to be processed: {}'.format(msg))


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
                                           'description': 'Start date. If value is none then today date is taken. Format:YYYY-MM-DD',
                                           'type': 'string'}
            end_date = 'None'
            config_params['end_date'] = {'title': 'End Date',
                                           'description': 'End date. If value is none then today date is taken. Format:YYYY-MM-DD',
                                           'type': 'string'}


count = 0

def process(msg):
    global count
    att_dict = msg.attributes

    att_dict['operator'] = 'filter_date'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()
    today_str = time.strftime("%Y-%m-%d")
    logger.debug("Today string: {}".format(today_str))

    start_date_str = tfp.read_value(api.config.start_date)
    if start_date_str == None :
        start_date_str = today_str
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    end_date_str = tfp.read_value(api.config.end_date)
    if end_date_str == None :
        end_date_str = today_str
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    filename = msg.attributes['file']['path']
    logger.debug('Attributes: {}'.format(str(msg.attributes)))
    logger.debug('Body: {}'.format(str(msg.body)))
    logger.info('Filename: {}'.format(filename))

    file_date = None
    if re.search('\d{4}-\d{2}-\d{2}',filename) :
        file_date_str = re.search('\d{4}-\d{2}-\d{2}',filename).group(0)
        logger.info('Date string found in filename: {}'.format(file_date_str))
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
    elif re.search('(\d{4})(\d{2})(\d{2})', filename):
        dates = re.search('(\d{8})(\d{2})(\d{2})', filename)
        date_found = False
    else :
        logger.info('No date pattern found in filename: {}'.format(filename))

    if file_date and file_date >= start_date and file_date <= end_date :
        count +=1
        logger.info('Date in filename is within given range: {} ({} - {})'.format(file_date_str,start_date_str,end_date_str))
        fmsg = api.Message(attributes = msg.attributes, body=filename)
        api.send(outports[2]['name'],fmsg)


    if msg.attributes['message.lastBatch']:
        api.send(outports[1]['name'], count)
        logger.info('Number of files: {}'.format(count))

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'input', 'type': 'message.file',"description":"Filename"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"},\
            {'name': 'numfiles', 'type': 'int64',"description":"Number of files"},\
            {'name': 'output', 'type': 'message.file',"description":"Filename"}]


api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    config.start_date = '2020-02-01'
    config.end_date = '2020-03-31'
    api.set_config(config)

    process(api.Message(attributes={'file':{'path':'/folder/file-2020-02-06'},'message.lastBatch':True},body = []))
    process(api.Message(attributes={'file':{'path':'/folder/file-20200207'},'message.lastBatch':True},body = []))
    process(api.Message(attributes={'file':{'path':'/folder/file-20200515'},'message.lastBatch':True},body = []))

