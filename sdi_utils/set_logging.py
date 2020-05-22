import logging
from io import StringIO

map_logs = {'CRITICAL':50,'ERROR':40,'WARNING':30,'INFO':20,'DEBUG':10,'NOTSET':0}

### set logger for strerr and to StringIO
def set_logging(name='operator',loglevel = logging.INFO, stream_output = True) :

    # to prevent the necessity to import logging only for passing the loglevel a string argument is

    log_level = logging.INFO
    if isinstance(loglevel,str) :
        try :
            log_level = map_logs[loglevel]
        except ValueError :
            raise ValueError('Unknown logging level. Valid values: INFO, DEBUG, WARNING, ERROR but not {}'.format(loglevel))
    elif isinstance(loglevel, bool) and loglevel == True  :
        log_level = logging.DEBUG
    elif isinstance(loglevel, int) :
        log_level = loglevel
    else:
        raise ValueError('Unknown logging level {}'.format(loglevel))

    format = '%(asctime)s ;  %(levelname)s ; %(name)s ; %(message)s'
    logging.basicConfig(level=log_level,format=format,datefmt='%H:%M:%S')

    logger = logging.getLogger(name=name)
    logger.setLevel(loglevel)
    log_stream = StringIO()
    # stream
    if stream_output :
        sh = logging.StreamHandler(stream=log_stream)
        sh.setFormatter(logging.Formatter(format, datefmt='%H:%M:%S'))
        sh.setLevel(log_level)
        logger.addHandler(sh)
        logger.info('Logger setup')
    logger.info('Logging Level: {}'.format(log_level))

    # stderr
    #if stderr_output :
    #    eh = logging.StreamHandler(stream=sys.stderr)
    #    eh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
    #    logger.addHandler(eh)

    return logger, log_stream

def main() :
    logger, log_stream = set_logging(name = 'Test', loglevel=True)
    logger.debug('Message debug level')
    for h in logger.handlers :
        h.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        #logger.addHandler(h)
    logger.debug('Message debug level')
    logger.info('Message info level')

    print('Log String: ')
    print(log_stream.getvalue())

if __name__ == '__main__':
    main()

