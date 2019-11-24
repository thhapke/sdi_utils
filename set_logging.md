# set_logging
Forks logging also to a string for tap wiring it with a separate monitor, e.g. 'Wire Tap'-Operator or 'Terminal'-Operator. 

# Example: 

```
import sdi_utils

logger, log_stream = log.set_logging(name='MyOperator',loglevel='DEBUG')

logger.info('Operator script started')
logger.debug('Output debug')
```

Output: 

```
15:14:44 - MyOperator - INFO - Operator script started
15:14:44 - MyOperator - DEBUG - Output debug
```
