import io

import os
import time



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
                print(msg.body)
            elif port == outports[2]['name'] :
                print('Limit reached - Exit')
                exit(0)
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Repl. Table Dispatcher"
            operator_name = 'repl_table_dispatcher'
            operator_description_long = "Send next table to replication process."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            periodicity = 0
            config_params['periodicity'] = {'title': 'Periodicity (s)',
                                       'description': 'Periodicity (s).',
                                       'type': 'integer'}

            parallelization = 0.1
            config_params['parallelization'] = {'title': 'Fraction of tables to parallelize',
                                       'description': 'Periodicity (s).',
                                       'type': 'number'}

            round_trips_to_stop = 10000000
            config_params['round_trips_to_stop'] = {'title': 'Roundtips to stop',
                                       'description': 'Fraction of tables to parallelize.',
                                       'type': 'integer'}


repl_tables = list()
pointer = 0
no_changes_counter = 0
num_roundtrips = 0
num_batch = 1
first_call = True

def set_replication_tables(msg) :
    global repl_tables
    global first_call
    global num_batch
    repl_tables = msg.body
    msg.attributes['num_tables'] = len(repl_tables)
    msg.attributes['data_outcome'] = True
    num_batch = int(api.config.parallelization * len(repl_tables))
    msg.attributes['num_batches'] = num_batch
    process(msg)

def process(msg) :

    global repl_tables
    global pointer
    global no_changes_counter
    global num_roundtrips
    global first_call
    global num_batch

    att = dict()

    att['operator'] = 'repl_table_dispatcher'
    att['data_outcome'] = msg.attributes['data_outcome'] if 'data_outcome' in msg.attributes else True

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    logger.debug('Attributes: {} - {}'.format(str(msg.attributes),str(att)))
    time_monitor = tp.progress()

    # case no repl tables provided
    if len(repl_tables) == 0 :
        logger.warning('No replication tables yet provided!')
        api.send(outports[0]['name'], log_stream.getvalue())
        return 0

    # counting when no table changes has been detected
    no_changes_counter = no_changes_counter + 1 if not att['data_outcome'] else 0

    # end pipeline if there were no changes in all tables AND happened more than round_trips_to_stop
    if no_changes_counter >= api.config.round_trips_to_stop * len(repl_tables):
        logger.info('Number of roundtrips without changes: {}'.format(no_changes_counter))
        logger.info('Process ended: {}'.format(time_monitor.elapsed_time()))
        api.send(outports[0]['name'], log_stream.getvalue())
        msg = api.Message(attributes=att, body=no_changes_counter)
        api.send(outports[2]['name'], msg)

    # goes idle if no changes has happened
    if pointer == 0 and not first_call:
        if num_roundtrips > 1:
            logger.info('******** {} **********'.format(num_roundtrips))
            logger.info(
                'Roundtrip completed: {} tables - {} unchanged roundtrips'.format(len(repl_tables), no_changes_counter))
            if no_changes_counter >= len(repl_tables) :
                logger.info('Goes idle due to no changes: {} s'.format(api.config.periodicity))
                time.sleep(api.config.periodicity)
        num_roundtrips += 1

    # parallelization
    #n Only needed when started. Then a new replication process is started whenever a table replication has been finished.
    if first_call :
        logger.debug('Parallelize: {} -> {}'.format(api.config.parallelization, num_batch))
        first_call = False
    else :
        num_batch = 1

    # Sends output message within a loop but less than number of replicated tables
    # but only on first call.
    # Later the loop back message comes and triggers 1 output message
    for b in range(0, num_batch) :

        # table dispatch carousel
        repl_table = repl_tables[pointer]
        pointer = (pointer + 1) % len(repl_tables)

        att['latency'] = repl_table['LATENCY']
        att['table'] = repl_table['TABLE']
        # split table from schema
        if '.' in repl_table['TABLE']  :
            att['base_table'] = repl_table['TABLE'].split('.')[1]
        else :
            att['base_table'] = repl_table['TABLE']
        table_msg = api.Message(attributes= att,body = {'TABLE':att['table'],'LATENCY':att['latency']})
        api.send(outports[1]['name'], table_msg)

        logger.info('Dispatch table: {} ({})'.format(att['table'],time_monitor.elapsed_time()))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()


inports = [{'name': 'tables', 'type': 'message',"description":"List of tables"},
           {'name': 'trigger', 'type': 'message',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'trigger', 'type': 'message',"description":"trigger"},
            {'name': 'limit', 'type': 'message',"description":"limit"}]


#api.set_port_callback(inports[1]['name'], process)
#api.set_port_callback(inports[0]['name'], set_replication_tables)

def test_operator() :

    api.config.debug_mode = True
    api.config.round_trips_to_stop = 1
    api.config.parallelization = 1

    data = [{'TABLE':'repl_TABLE1', 'LATENCY':0},{'TABLE':'repl_TABLE2', 'LATENCY':0},{'TABLE':'repl_TABLE3', 'LATENCY':0},
            {'TABLE':'repl_TABLE4', 'LATENCY':0},{'TABLE':'repl_TABLE5', 'LATENCY':0},{'TABLE':'repl_TABLE6', 'LATENCY':0}]

    msg = api.Message(attributes={'tables':'test_tables'}, body=data)
    set_replication_tables(msg)

    trigger = api.Message(attributes={'table':'test','data_outcome':False}, body='go')
    for i in range(0,20) :
        process(trigger)

if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



