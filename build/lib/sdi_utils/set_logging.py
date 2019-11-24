import logging
from io import StringIO
import sys

map_logs = {'DEBUG':logging.DEBUG,'INFO':logging.INFO,'WARNING':logging.WARNING,'ERROR':logging.ERROR}

### set logger for strerr and to StringIO
def set_logging(loglevel = logging.INFO) :
    if isinstance(loglevel,str) :
        try :
            loglevel = map_logs[loglevel]
        except KeyError :
            raise ValueError('Unknown logging level. Valid values: INFO, DEBUG, WARNING, ERROR')
    log_stream = StringIO()
    logger = logging.getLogger()
    sh = logging.StreamHandler(stream=log_stream)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(sh)
    eh = logging.StreamHandler(stream=sys.stderr)
    eh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(eh)
    logger.setLevel(loglevel)
    return logger, log_stream
