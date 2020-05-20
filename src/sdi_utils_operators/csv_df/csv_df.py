import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import pandas as pd
import subprocess
import io
import re
import os

EXAMPLE_ROWS = 5

try:
    api
except NameError:

    class api:

        queue = list()
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[0]['name']:
                api.queue.append(msg)

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            version = "0.0.17"
            tags = {'pandas': '','sdi_utils':''}
            operator_name = 'csv_df'
            operator_description = "From CSV to DataFrame"
            operator_description_long = "Creating a DataFrame with csv-data passed through inport."
            add_readme = dict()
            add_readme[
                "References"] = "[pandas doc: read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)"

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode', 'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data', 'description': 'Collect data before sending it to the output port',
                                           'type': 'boolean'}
            index_cols = 'None'
            config_params['index_cols'] = {'title': 'Index Columns', 'description': 'Index columns of dataframe',
                                           'type': 'string'}
            separator = ','
            config_params['separator'] = {'title': 'Separator of CSV', 'description': 'Separator of CSV',
                                          'type': 'string'}
            use_columns = 'None'
            config_params['use_columns'] = {'title': 'Use columns from CSV',
                                            'description': 'Use columns from CSV (list)', 'type': 'string'}
            limit_rows = 0
            config_params['limit_rows'] = {'title': 'Limit number of rows',
                                           'description': 'Limit number of rows for testing purpose', 'type': 'number'}
            downcast_int = False
            config_params['downcast_int'] = {'title': 'Downcast integers',
                                             'description': 'Downcast integers from int64 to int with smallest memory footprint',
                                             'type': 'boolean'}
            downcast_float = False
            config_params['downcast_float'] = {'title': 'Downcast float datatypes',
                                               'description': 'Downcast float64 to float32 datatypes',
                                               'type': 'boolean'}
            df_name = 'DataFrame'
            config_params['df_name'] = {'title': 'DataFrame name',
                                        'description': 'DataFrame name for debugging reasons', 'type': 'string'}

            decimal = '.'
            config_params['decimal'] = {'title': 'Decimals separator', 'description': 'Decimals separator',
                                        'type': 'string'}
            dtypes = 'None'
            config_params['dtypes'] = {'title': 'Data Types of Columns',
                                       'description': 'Data Types of Columns (list of maps)', 'type': 'string'}
            data_from_filename = 'None'
            config_params['data_from_filename'] = {'title': 'Data from Filename', 'description': 'Data from Filename',
                                                   'type': 'string'}
            todatetime = 'None'
            config_params['todatetime'] = {'title': 'To Datetime', 'description': 'To Datetime', 'type': 'string'}
            utc = True
            config_params['utc'] = {'title': 'Use UTC', 'description': 'If true utc is used for time conversion', 'type': 'boolean'}

            keyword_args = "'error_bad_lines'= True, 'low_memory' = False, compression = None, thousands = None "
            config_params['keyword_args'] = {'title': 'Keyword Arguments',
                                             'description': 'Mapping of key-values passed as arguments \"to read_csv\"',
                                             'type': 'string'}


def downcast(df, data_type, to_type):
    cols = list(df.select_dtypes(include=[data_type]).columns)
    if len(cols) == 0:
        return df, None

    downcast_dict = dict()
    downcast_dict['data_type'] = data_type
    cdtypes = df[cols].dtypes.to_dict()
    downcast_dict['previous_subtypes'] = {col: str(itype) for col, itype in cdtypes.items()}
    downcast_dict['previous_mem_usage'] = df[cols].memory_usage(deep=True).sum() / 1024 ** 2

    df[cols] = df[cols].apply(pd.to_numeric, downcast=to_type)

    int_dtypes2 = df[cols].dtypes.to_dict()
    downcast_dict['subtypes'] = {col: str(itype) for col, itype in int_dtypes2.items()}
    downcast_dict['mem_usage'] = df[cols].memory_usage(deep=True).sum() / 1024 ** 2

    return df, downcast_dict

result_df = pd.DataFrame()

def process(msg):

    global result_df

    att_dict = msg.attributes

    att_dict['operator'] = 'dict_table'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode,stream_output=True)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(msg.attributes)))

    if isinstance(msg.body, str):
        csv_io = io.StringIO(msg.body)
        logger.debug("Input format: <string>")
    elif isinstance(msg.body, bytes):
        csv_io = io.BytesIO(msg.body)
        logger.debug("Input format: <bytes>")
    elif isinstance(msg.body, io.BytesIO):
        logger.debug("Input format: <io.Bytes>")
        csv_io = msg.body
    else:
        raise TypeError('Message body has unsupported type' + str(type(msg.body)))

    # nrows
    nrows = None
    if not api.config.limit_rows == 0:
        nrows = api.config.limit_rows

    # usecols
    use_cols = tfp.read_list(api.config.use_columns)
    logger.debug('Columns used: {}'.format(use_cols))

    # dtypes mapping
    typemap = tfp.read_dict(api.config.dtypes)
    logger.debug('Type cast: {}'.format(str(typemap)))

    kwargs = tfp.read_dict(text=api.config.keyword_args, map_sep='=')

    ##### Read string from buffer
    logger.debug("Read from input")
    df = pd.read_csv(csv_io, api.config.separator, usecols=use_cols, dtype=typemap, decimal=api.config.decimal, \
                     nrows=nrows, **kwargs)

    # Data from filename
    if api.config.data_from_filename and not api.config.data_from_filename == 'None':
        col = api.config.data_from_filename.split(':')[0].strip().strip("'").strip('"')
        pat = api.config.data_from_filename.split(':')[1].strip().strip("'").strip('"')
        logger.debug('Filename: {}  pattern: {}'.format(att_dict['filename'], pat))
        try:
            dataff = re.match('.*(\d{4}-\d+-\d+).*', att_dict['filename'])
            df[col] = dataff.group(1)
        except AttributeError:
            raise ValueError('Pattern not found - Filename: {}  pattern: {}'.format(att_dict['filename'], pat))

    # To Datetime
    if api.config.todatetime and not api.config.todatetime == 'None':
        dt_fmt = tfp.read_dict(api.config.todatetime)
        logger.debug('Time conversion {} by using UTC {}'.format(api.config.todatetime,api.config.utc))
        for col, fmt in dt_fmt.items() :
            df[col] = pd.to_datetime(df[col], format=fmt, utc= api.config.utc)

    ###### Downcasting
    # save memory footprint for calculating the savings of the downcast
    logger.debug('Memory used before downcast: {}'.format(df.memory_usage(deep=True).sum() / 1024 ** 2))
    if api.config.downcast_int:
        df, dci = downcast(df, 'int', 'unsigned')
    if api.config.downcast_float:
        df, dcf = downcast(df, 'float', 'float')

    # check if index is provided and set
    index_list = tfp.read_list(api.config.index_cols)
    if index_list:
        df.set_index(index_list, inplace=True)

    if not result_df.empty :
        result_df = pd.concat([result_df, df], axis=0, sort=False)
    else :
        result_df = df

    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Memory: {} kB'.format(df.memory_usage(deep=True).sum() / 1024 ** 2))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        logger.debug('Row {}: {}'.format(i,str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])))

    logger.debug('Process ended: {}  '.format(time_monitor.elapsed_time()))

    api.send(outports[0]['name'],log_stream.getvalue())

    logger.info('Collecting incoming data: {}'.format(api.config.collect))
    if not api.config.collect or ('message.lastBatch' in msg.attributes and msg.attributes['message.lastBatch'] == True) :
        logger.debug('Send data to outport')
        api.send(outports[1]['name'], api.Message(attributes=att_dict, body=result_df))


inports = [{'name': 'csv', 'type': 'message.file',"description":"Input byte or string csv"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame',"description":"Output data"}]


#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    config = api.config
    config.debug_mode = True
    config.use_columns = "'Exportdatum','Postleitzahl','Ort','Ortsteil','Verbrauchsstufe','Rang','Gesamtpreis','Anbietername'"
    config.downcast_float = True
    config.downcast_int = True
    config.dtypes = "'Gesamtpreis':'float32','Postleitzahl':'uint32','Verbrauchsstufe':'uint16'"
    config.separator = ';'
    config.index_cols = "None"
    config.limit_rows = 0
    config.df_name = 'DataFrame'
    config.decimal = '.'
    config.utc = False
    config.collect = False
    config.todatetime = 'Exportdatum : %Y-%m-%d'
    config.keyword_args = "'error_bad_lines'= True, 'low_memory' = False, compression = None, comment = '#'"

    api.set_config(config)

    in_dir = '/Users/Shared/data/OptRanking/portal1_samples25'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*csv', f)]

    for i, fname in enumerate(files_in_dir):

        if i == 5 :
            break

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'message.lastBatch':eos}
        csv = open(os.path.join(in_dir, fname), mode='rb').read()
        msg = api.Message(attributes=attributes, body=csv)

        process(msg)

if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

