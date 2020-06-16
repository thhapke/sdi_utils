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
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def set_config(config):
            api.config = config
                
        def send(port,msg) :
            if port == outports[1]['name'] :
                print('ATTRIBUTES: ')
                print(msg.attributes)#
                print('CSV-String: ')
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'table_csv'
            operator_description = "table to csv stream"
            operator_description_long = "Converts table to csv stream."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            separator = ';'
            config_params['separator'] = {'title': 'Separator', 'description': 'Separator',
                                           'type': 'string'}


def process(msg):
    att_dict = msg.attributes

    att_dict['operator'] = 'table_csv'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started")
    time_monitor = tp.progress()

    header = [c["name"] for c in msg.attributes['table']['columns']]

    csv_io = io.StringIO()
    writer = csv.writer(csv_io)
    writer.writerow(header)
    writer.writerows(msg.body)
    msg = api.Message(attributes=att_dict,body = csv_io.getvalue())
    api.send(outports[1]['name'],msg)

    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'data', 'type': 'message.table',"description":"Input message with table"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'csv', 'type': 'message',"description":"Output data as csv"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator() :
    config = api.config
    config.debug_mode = True
    config.separator = ','
    api.set_config(config)


    attributes = {"table":{"columns":[{"class":"string","name":"header1","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header2","nullable":True,"size":3,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"header3","nullable":True,"size":10,"type":{"hana":"NVARCHAR"}}],
                           "name":"test.table","version":1}}
    table = [ [(j*3 + i) for i in range(0,3)] for j in range (0,5)]
    msg = api.Message(attributes=attributes, body=table)
    print(table)
    process(msg)


if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
