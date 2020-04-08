import re

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
                
        def send(port,str) :
            print('Port send: {}'.format(str))
            return str
    
        def call(config,str):
            api.config = config
            return process(str)
            
        def set_port_callback(port, callback) :
            pass
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'pandas': '','sdi_utils':''}
            version = "0.0.1"
            operator_description = "Parse Logs"
            operator_description_long = "Parse logs for keywords to control process flow, e.g. stop a branched pipeline consistently"
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            marker_map = 'operator_name1: keyword1 ; operator_name2: keyword2'
            config_params['marker_map'] = {'title': 'List of marker','description': \
                'List of keywords of operators that send an integer to output','type': 'string'}
            sleep = 5
            config_params['sleep'] = {'title': 'Sleep','description': \
                'Time before starting next processing step','type': 'integer'}
            separator = ';'
            config_params['separator'] = {'title': 'Separator','description': \
                'Separates fields of logging stream','type': 'string'}



def process(log_str) :

    # start custom process definition
    parse_result = None

    marker = tfp.read_dict(api.config.marker_map)
    operators = marker.keys()

    lines = log_str.splitlines()
    for l in lines :
        fields = l.split(api.config.separator)
        for m in  marker.keys() :
            if fields[2].strip() == m  :
                pat = f'.*<{marker[m]}><(\d+)>'
                match_key = re.match(f'.*<{marker[m]}><(\d+)>',fields[3])
                if match_key :
                    index = match_key.group(1)
                    api.send(outports[1]['name'],str(index))
                    api.send(outports[2]['name'],int(index))


inports = [{"name":"log","type":"string","description":"Input log statements"}]
outports = [{"name":"log","type":"string","description":"Output of unchanged log statements"},\
            {"name":"string_string","type":"string","description":"Control string"},\
            {"name":"index","type":"int64","description":"Control index"}]

def call_on_input(log) :
    api.send(outports[0]['name'], log)
    process(log)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    config = api.config
    config.separator = ';'
    config.sleep = 1
    config.marker_map = 'fromCSV : BATCH ENDED, dropColumns : BATCH IN PROCESS'

    test_str = r"""10:20:06 ; INFO ; fromCSV ; Process started
10:20:06 ; INFO ; fromCSV ; Filename: AT_NO2.csv index: 1 count: 2 endofSeq: True
10:20:06 ; DEBUG ; fromCSV ; Process ended: <BATCH ENDED><2> - 0m 0.178s ( 0: 0:0.2)
10:20:06 ; DEBUG ; fromCSV ; Process ended: <BATCH IN PROCESS> <1>- 0m 0.178s ( 0: 0:0.2)
10:20:06 ; INFO ; dropColumns ; Process started
10:20:06 ; DEBUG ; dropColumns ; Columns: Index(['country_code', 'pollutant', 'x', 'y', 'coordsys', 'datetime', 'value', 'altitude'], dtype='object')
10:20:06 ; DEBUG ; dropColumns ; Process ended: <BATCH IN PROCESS> <2>- 0m 0.178s ( 0: 0:0.2)
10:20:06 ; DEBUG ; dropColumns ; Past process steps: ['EEA_pollution_getdata', 'dropColumns']
"""
    api.call(config=config, str = test_str)


if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
