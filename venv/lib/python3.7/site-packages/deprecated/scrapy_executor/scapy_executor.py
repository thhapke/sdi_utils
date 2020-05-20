import subprocess
import os

import sdi_utils.gensolution as gs
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
            if isinstance(msg, api.Message):
                print('Attributes\n{}: {}'.format(port, msg.attributes))
                print('Body\{}: {}'.format(port, msg.body))
            else:
                print('{}: {}'.format(port, msg))

        def call(config):
            api.config = config
            process()

        class config:
            ## Meta data
            config_params = dict()
            tags = {'pandas': '', 'sdi_utils': '', 'scrapy':''}
            version = "0.0.1"
            operator_description = "scrapy executor"
            operator_description_long = "Starts scrapy and sends output to data port and stderr to log port."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            cmd = 'None'
            config_params['cmd'] = {'title': 'Command', 'description': \
                'Command to be executed', 'type': 'string'}

            scrapy_dir = 'None'
            config_params['scrapy_dir'] = {'title': 'Scrapy Directory', 'description': \
                'Scrapy directory on container', 'type': 'string'}

inports = [{"name": "trigger", "type": "string", "description": "Trigger"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging"},\
            {'name': 'stderr', 'type': 'string', "description": "Stderr"}, \
            {"name": "data", "type": "message", "description": "data"}]

def process(str) :

    logger, log_stream = slog.set_logging('scrapy_executor',loglevel=api.config.debug_mode)

    scrapy_dir = tfp.read_value(api.config.scrapy_dir)
    if scrapy_dir :
        logger.info('Change directory to: {}'.format(scrapy_dir))
        os.chdir(scrapy_dir)

    cmd = tfp.read_list(api.config.cmd,sep=' ')
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)

    output = '1'
    errout = '1'
    while proc.poll() is None or  output != '' or  errout != '':
        output = proc.stdout.readline()
        errout = proc.stderr.readline()
        if output != '':
            att_dict = {'article':'test'}
            msg = api.Message(attributes=att_dict,body=output)
            api.send(outports[2]['name'],msg)
        if errout != '':
            api.send(outports[1]['name'],errout)

#api.set_port_callback(inports[0]['name'], process)


def main():
    print('CWD: {}'.format(os.getcwd()))
    config = api.config
    config.debug_mode = True
    config.scrapy_dir = '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/src/sdi_utils_operators/scrapy_executor'
    config.cmd = 'python /tmp/outputgen.py'
    api.call(config)

if __name__ == '__main__':
    main()


