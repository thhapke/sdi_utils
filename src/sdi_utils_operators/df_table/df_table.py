import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


import subprocess
import os
import pandas as pd



try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            pass
    
        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'pandas': '','sdi_utils':''}
            operator_name = 'df_table'
            operator_description = "df to table"
            operator_description_long = "Produces a 2 dim array from DataFrame."
            add_readme = dict()
            add_readme["References"] = r"""[pandas doc: to_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html)"""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            reset_index = False
            config_params['reset_index'] = {'title': 'Reset Index', 'description': 'Reset index or indices', 'type': 'boolean'}

            table_name = 'table'
            config_params['table_name'] = {'title': 'Table Name', 'description': 'Table name of DB', 'type': 'string'}


def process(msg) :

    att_dict = msg.attributes

    att_dict['operator'] = 'df_table'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode,stream_output=True)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(msg.attributes)))

    # start custom process definition
    df = msg.body
    if api.config.reset_index :
        logger.debug('Reset Index')
        df = df.reset_index()

    table_name = tfp.read_value(api.config.table_name)
    if not table_name :
        table_name = 'Default_table'
        if 'file' in msg.attributes and 'path' in msg.attributes['file'] :
            name = os.path.basename(msg.attributes['file']['path']).split('.')[0]
    att_dict["table"] = {"name": table_name , "version": 1, "columns": list()}
    for col in df.columns:
        att_dict["table"]["columns"].append({"name": col})

    # Datetime
    col_dt = df.select_dtypes(include=['datetime64[ns, UTC]','datetime64[ns]','datetime64']).columns
    for col in col_dt :
        df[col] = df[col].dt.strftime('%Y-%m-%d')
    #print(df.dtypes)
    #print(df)
    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))

    print(df.values.tolist())

    api.send(outports[0]['name'],log_stream.getvalue())
    api.send(outports[1]['name'],api.Message(attributes=att_dict,body=df.to_numpy().tolist()))



inports = [{'name': 'data', 'type': 'message.DataFrame',"description":"Input data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'table', 'type': 'message.table',"description":"Output data as table"}]



#api.set_port_callback(inports[0]['name'], process)

def test_operator() :

    config = api.config
    config.write_index = False
    config.reset_index = True
    api.set_config(config)

    df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': ['2020-01-01', '2020-02-01', '2020-01-31', '2020-01-28','2020-04-12'],\
                       'col3': [100.0, 200.2, 300.4, 400, 500],'names':['Anna','Berta','Berta','Claire','Dora']})
    df = df.set_index(keys=['icol'])
    df['col 2'] = pd.to_datetime(df['col 2'],format='%Y-%m-%d',utc=True)

    attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}
    msg = api.Message(attributes=attributes,body=df)

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
        
