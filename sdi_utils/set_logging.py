import logging
from io import StringIO


map_logs = {'DEBUG':logging.DEBUG,'INFO':logging.INFO,'WARNING':logging.WARNING,'ERROR':logging.ERROR,'CRITICAL':logging.CRITICAL}

### set logger for strerr and to StringIO
def set_logging(name='operator',loglevel = logging.INFO, stream_output = True) :

    # to prevent the necessity to import logging only for passing the loglevel a string argument is given
    if isinstance(loglevel,str) :
        try :
            loglevel = map_logs[loglevel]
        except KeyError :
            raise ValueError('Unknown logging level. Valid values: INFO, DEBUG, WARNING, ERROR')

    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=loglevel,format=format,datefmt='%H:%M:%S')

    logger = logging.getLogger(name=name)
    logger.setLevel(loglevel)
    log_stream = StringIO()
    # stream
    if stream_output :
        sh = logging.StreamHandler(stream=log_stream)
        sh.setFormatter(logging.Formatter(format, datefmt='%H:%M:%S'))
        logger.addHandler(sh)

    # stderr
    #if stderr_output :
    #    eh = logging.StreamHandler(stream=sys.stderr)
    #    eh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
    #    logger.addHandler(eh)

    return logger, log_stream

def main() :
    logger, log_stream = set_logging(name = 'Test', loglevel='DEBUG')
    logger.debug('Message debug level')
    logger.info('Message info level')

    print('Log String: ')
    print(log_stream.getvalue())

if __name__ == '__main__':
    main()

