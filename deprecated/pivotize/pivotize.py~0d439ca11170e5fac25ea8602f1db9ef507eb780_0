
import csv

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

        def send(port, msg):
            if port == outports[1]['name']:
                print('PORT: {}'.format(port))
                print(msg.attributes)
                print(msg.body)

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "Pivotize table"
            operator_description_long = "Sends input topics to SQL processor and topic frequency operator."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            ref_col_index = 0
            config_params['ref_col_index'] = {'title': 'Reference column index',
                                           'description': 'Reference column index',
                                           'type': 'integer'}
            pivot_cols_index = '2, 3'
            config_params['pivot_cols_index'] = {'title': 'Pivot columns index list',
                                           'description': 'Pivot columns index list',
                                           'type': 'string'}

            pivot_column_name = 'PIVOT_COLUMN'
            config_params['pivot_column_name'] = {'title': 'New Colum Name',
                                                 'description': 'New Colum Name',
                                                 'type': 'string'}

def process(msg):
    logger, log_stream = slog.set_logging('pivotize', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    table = msg.body

    pivot_col_name = api.config.pivot_column_name
    ref_col_index = api.config.ref_col_index
    pivot_cols = tfp.read_list(api.config.pivot_cols_index,test_number=True)
    for i in pivot_cols :
        if not isinstance(i,int) :
            raise Exception('Config parameter needs to be list of integers')

    logger.debug('Table to pivot: {} - {} with {} columns'.format(len(table),len(table[0]),len(pivot_cols)))

    table = msg.body
    att_dict = msg.attributes
    ref_column = att_dict['table']['columns'][ref_col_index]
    pivot_col = att_dict['table']['columns'][pivot_cols[0]]
    pivot_col['name'] = pivot_col_name
    att_dict['table']['columns'] = [ref_column,pivot_col]

    pivot_table = list()
    for row in table :
        pivot_table.extend([ [row[ref_col_index],row[c]] for c in pivot_cols ])

    # remove empty rows
    pivot_table = [r for r in pivot_table if not r[1] == '' and not r[1] == None]

    logger.debug('Pivot Table: {} - {}'.format(len(pivot_table), len(pivot_table[0])))
    logger.debug('Process ended, rows processed {}  - {}  '.format(len(table), time_monitor.elapsed_time()))
    msg = api.Message(attributes=att_dict,body = pivot_table)
    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'table', 'type': 'message.table', "description": "Message with pivotized table"}]
inports = [{'name': 'topic', 'type': 'message.table', "description": "Message with body as table."}]


#api.set_port_callback(inports[0]['name'], process)


def main():
    topics_filename = '/Users/Shared/data/onlinemedia/repository/topics.csv'
    topics = list()
    with open(topics_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        next(rows,None)
        for r in rows:
            topics.append(r)

    attributes = {"table":{"columns":[{"class":"string","name":"TOPIC","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"LANGUAGE","nullable":True,"size":3,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"TYPE","nullable":True,"size":10,"type":{"hana":"NVARCHAR"}},
                                      {"class":"timestamp","name":"DATE","nullable":True,"type":{"hana":"DATE"}},
                                      {"class":"timestamp","name":"EXPERIY_DATE","nullable":True,"type":{"hana":"DATE"}},
                                      {"class":"string","name":"ATTRIBUTE","nullable":True,"size":15,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_1","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_2","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_3","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_4","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_5","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_6","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_7","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_8","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_9","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"KEYWORD_10","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}}],
                           "name":"DIPROJECTS.TOPICS","version":1}}
    topics_msg = api.Message(attributes=attributes, body=topics)
    config = api.config
    config.ref_col_index = 0
    config.pivot_cols_index = "6,7,8,9,10,11,12,13,14,15"
    config.debug_mode = True
    api.set_config(config)
    process(topics_msg)

if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)





