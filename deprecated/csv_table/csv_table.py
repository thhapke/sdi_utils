import io
import subprocess
import os
import csv

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def set_config(config):
            api.config = config

        def send(port, msg):
            if port == outports[1]['name']:
                print(msg.attributes)
                for i, dk in enumerate(msg.body):
                    print(dk)
                    if i > 10:
                        break
                header = [c["name"] for c in msg.attributes['table']['columns']]
                print(header)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "csv to table"
            operator_name = 'csv_table'
            operator_description_long = "Converts csv to table"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data',
                                        'description': 'Collect data before sending it to the output port',
                                        'type': 'boolean'}
            has_header = True
            config_params['has_header'] = {'title': 'Has_header',
                                           'description': 'If csv-file has a header.',
                                           'type': 'boolean'}
            separator = ';'
            config_params['separator'] = {'title': 'Separator', 'description': 'Separator',
                                          'type': 'string'}

result_table = list()


def process(msg):
    att_dict = msg.attributes

    global result_table

    # ONLY DUE TO BUG
    changed_debug = False
    changed_header = False
    if api.config.debug_mode == False:
        api.config.debug_mode = True
        changed_debug = True
    if api.config.has_header == False:
        api.config.has_header = True
        changed_header = True

    att_dict['operator'] = 'csv_table'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    # ONLY DUE TO BUG
    if changed_debug:
        logger.debug('Logger level changed to DEBUG')
    if changed_header:
        logger.debug('has_header changed to True')

    logger.info("Process started")
    time_monitor = tp.progress()

    logger.debug('Attributes: {}'.format(str(att_dict)))

    str_body = msg.body.decode('utf-8')
    csv_io = io.StringIO(str_body)

    rows = csv.reader(csv_io, delimiter=api.config.separator)
    # first row
    row1 = next(rows)
    if not api.config.has_header:
        result_table.append(row1)
    for r in rows:
        result_table.append(r)

    att_dict["table"] = {"name": msg.attributes['file']['path'], "version": 1, "columns": list()}
    for i, h in enumerate(row1):
        if api.config.has_header:
            att_dict["table"]["columns"].append({"name": h})
        else:
            att_dict["table"]["columns"].append({"name": 'column_' + str(i)})

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))
    out_msg = api.Message(attributes=att_dict, body=result_table)
    if 'message.lastBatch' in msg.attributes:
        if msg.attributes['message.lastBatch'] or api.config.collect == False:
            api.send(outports[1]['name'], out_msg)
            result_table = list()
    else:
        api.send(outports[1]['name'], out_msg)

    if api.config.debug_mode:
        for i, dk in enumerate(result_table):
            logger.debug(dk)
            if i > 10:
                break

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'stream', 'type': 'message.file', "description": "Input csv byte or string"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "Output data as table"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.collect = False
    config.separator = ','
    config.has_header = False
    api.set_config(config)

    fname = '/Users/Shared/data/onlinemedia/repository/lexicon.csv'

    fbase = fname.split('.')[0]
    attributes = {'format': 'csv', "file": {'path': fname}, 'message.lastBatch': True, \
                  'storage.fileIndex': 0, 'storage.fileCount': 1, 'process_list': []}
    csvstream = open(fname, mode='rb').read()
    msg = api.Message(attributes=attributes, body=csvstream)
    process(msg)


if __name__ == '__main__':
    test_operator()
    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        print('Solution name: {}'.format(solution_name))
        print('Current directory: {}'.format(os.getcwd()))
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_0.0.1',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])
