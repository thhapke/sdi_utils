
import os
import re
import json
import subprocess
import requests
import logging
import argparse

from shutil import copyfile, move, rmtree

exclude_files = ['__init__.py']
exclude_dirs = ['__pycache__']

def gensolution(script_path, config, inports, outports, override_readme = False, tar = False) :

    script_path = os.path.abspath(script_path)
    drive, project_path = os.path.splitdrive(script_path)
    project_path, folder = os.path.split(project_path)
    while folder  and not folder == 'src':
        project_path, folder = os.path.split(project_path)

    src_path = os.path.join(project_path,"src")

    script_filename  = os.path.basename(script_path)

    # generate configSchema.json
    operator_path = os.path.dirname(script_path)
    operator_path_name = operator_path[len(src_path) + 1:]
    package_name = operator_path_name.split(os.path.sep, 1)[0]
    operator_id = operator_path_name.replace(os.path.sep, ".")
    idstr = operator_id + '.configSchema.json'
    configschema = dict()
    configschema["$schema"] =  "http://json-schema.org/draft-06/schema#"
    configschema["$id"] = "http://sap.com/vflow/" + idstr
    configschema["type"] =  "object"
    configschema["properties"] = {'codelanguage' :{'type' :'string'} ,'script' :{'type' :'string'}}
    for k ,v in config.config_params.items() :
        configschema['properties'][k] = v
    logging.info('Write configSchema.json')
    with open(os.path.join(operator_path, 'configSchema.json'), 'w') as schemafile:
        json.dump(configschema, schemafile, indent=4)

    # generate operator.json
    opjson = dict()
    opjson['description'] = config.operator_description
    opjson['component'] = "com.sap.system.python3Operator"
    opjson['inports'] = inports
    opjson['outports'] = outports
    opjson['config'] = {'$type' :configschema["$id"] ,'script' :'file://' +os.path.basename(script_filename)}
    for k ,v in config.config_params.items() :
        opjson['config'][k] = eval('config. ' +k)
    opjson['tags'] = config.tags
    svg_files = [f for f in os.listdir(operator_path) if os.path.isfile(os.path.join(operator_path, f)) and re.match('.*svg', f)]
    if svg_files :
        opjson["iconsrc"] = os.path.basename(svg_files[0])
    logging.info('Write operator.json')
    with open(os.path.join(operator_path, 'operator.json'), 'w') as opfile:
        json.dump(opjson, opfile, indent=4)

    # create README (but only if not exists to prevent overriding manual additions
    readme_file = os.path.join(operator_path, 'README.md')
    if not os.path.exists(readme_file) or override_readme :
        logging.info('Write README.md')
        readme = "# " +  operator_id + ' - ' + '\n\n'
        readme += "## Inport" + '\n\n'
        for ip in opjson['inports'] :
            readme += "* " + ip["name"] + '  (Type: ' +  ip["type"] + ")\n"
        readme += "\n"
        readme += "## outports" + '\n\n'
        for ip in opjson['outports'] :
            readme += "* " + ip["name"] + '  (Type: ' +  ip["type"] + ")\n"
        readme += "\n"
        readme += "## Config" + '\n\n'
        for ic in config.config_params.values() :
            readme += "* " + ic['title'] + "  (Type: " + ic['type'] + ")  -  " + ic['description'] + "\n"
        with open(readme_file, 'w') as rmfile:
            rmfile.write(readme)

    # create dirs and copy data
    operator_solution_path = os.path.join(project_path, 'solution', 'operators', package_name + '_' + config.version, 'content', \
                                          'files' ,'vflow' ,'subengines' ,'com' ,'sap', 'python36', 'operators', operator_path_name)
    os.makedirs(operator_solution_path, exist_ok=True)
    logging.info('Copy operator files to: ' + operator_solution_path)
    copyfile(os.path.join(operator_path,"README.md"),os.path.join(operator_solution_path,"README.md"))
    copyfile(os.path.join(operator_path, 'operator.json'), os.path.join(operator_solution_path, 'operator.json'))
    copyfile(os.path.join(operator_path, 'configSchema.json'), os.path.join(operator_solution_path, 'configSchema.json'))
    if svg_files :
        svg_file =  os.path.basename(svg_files[0])
        copyfile(os.path.join(operator_path, svg_file),os.path.join(operator_solution_path, svg_file))

    # Review code and do some adjustments
    # * remove comment in line #api.set_port_callback (used when run locally)
    # * remove all lines following "if __name__ == '__main__': "^
    # * remove import sdi_utils - maybe used in same script for generating
    with open(script_path, 'r') as read_fn:
        with open(os.path.join(operator_solution_path, script_filename), 'w') as write_fn:
            line = read_fn.readline()
            while line :
                line = re.sub(r'^#api.set_port_callback', 'api.set_port_callback', line)
                if re.match(r'if __name__ == \'__main__\'',line) or re.match(r'if __name__ == \"__main__\"',line) :
                    break
                #if re.match(r'import sdi_utils',line) :
                #    line = read_fn.readline()
                #    continue
                write_fn.write(line)
                line = read_fn.readline()
        write_fn.close()
    read_fn.close()

    # create manifest
    manifest = {"name": package_name, "version": config.version, "format": "2", "dependencies": []}
    root_solution_path = os.path.join(project_path, 'solution', 'operators')
    manifest_filename = os.path.join(root_solution_path, package_name +'_' + config.version, 'manifest.json')
    logging.info('Write manifest.json ')
    with open(manifest_filename, 'w') as manifestfile:
        json.dump(manifest, manifestfile, indent=4)

def change_version(manifest_file, version) :
    with open(manifest_file,'r') as json_file:
        manifest_dict = json.load(json_file)
        json_file.close()
    manifest_dict['version'] = version
    # versions needs to have a specific format 0.0.0

    with open(manifest_file,'w') as json_file:
        json.dump(manifest_dict, json_file,indent=4)
        json_file.close()

def download_templatefile(url,path) :
    logging.info("Download templateCode.py from Github {} to  {}".format(url,path))
    example_code = requests.get(url)
    open(path, 'wb').write(example_code.content)

###############################################################################################################
### REVERSE
### Load json files configSchema.json and operator.json
### Copy all files to src/operator folder
###############################################################################################################
def reverse_solution(project_path, package, operator_folder) :
    operator_folder = operator_folder.replace('.','/')
    operator_solution_path = os.path.join(project_path, 'solution', 'operators', package, 'content','files' ,'vflow', \
                                   'subengines' ,'com' ,'sap', 'python36', 'operators', operator_folder)
    operator_script = os.path.basename(operator_folder) + '.py'
    logging.debug("Load <configSchema.json>")
    with open(os.path.join(operator_solution_path,'configSchema.json')) as schemaFile:
        schemaj = json.load(schemaFile)
        schemaFile.close()

    logging.debug("Load <operator.json>")
    with open(os.path.join(operator_solution_path,'operator.json')) as operatorFile:
        operatorj = json.load(operatorFile)
        operatorFile.close()

    logging.debug("Make src-directory for operator")
    operator_src_path = os.path.join(project_path,'src',operator_folder)
    logging.debug("Make src-directory for operator: {}".format(operator_src_path))
    os.makedirs(operator_src_path, exist_ok=True)

    logging.debug('Copy files to src-directory for operator (README.md, operator.json, configSchema.json')
    copyfile(os.path.join(operator_solution_path, "README.md"), os.path.join(operator_src_path, "README.md"))
    copyfile(os.path.join(operator_solution_path, 'operator.json'), os.path.join(operator_src_path, 'operator.json'))
    copyfile(os.path.join(operator_solution_path, 'configSchema.json'),os.path.join(operator_src_path, 'configSchema.json'))
    copyfile(os.path.join(operator_solution_path, operator_script),os.path.join(operator_src_path, 'old_'+operator_script))

    with open(os.path.join(operator_src_path,operator_script),'w') as operatorFile :
        firstblock = r"""import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog

import os

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if isinstance(msg,api.Message) :
                print('Port: ', port)
                print('Attributes: ', msg.attributes)
                print('Body: ', str(msg.body))
            else :
                print(str(msg))
            return msg
    
        def call(config,msg):
            api.config = config
            return process(msg)
            
        def set_port_callback(port, callback) :
            default_msg = api.Message(attributes={'name':'doit'},body = 'message')
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
        """
        operatorFile.write(firstblock)
        operatorFile.writelines("{:4}tags = {}\n".format('',str(operatorj['tags'])))
        operatorFile.writelines("{:12}operator_description = \"{}\"\n".format('',os.path.basename(operator_folder)))
        for name,defs in schemaj['properties'].items() :
            if name == 'codelanguage' or name == 'script' :
                continue
            if defs['type'] == 'string' :
                operatorFile.writelines("{:12}{} = '{}'\n".format('', name, operatorj['config'][name]))
            else :
                operatorFile.writelines("{:12}{} = {}\n".format('',name,operatorj['config'][name]))
            operatorFile.writelines("{:12}config_params['{}'] = {}\n".format('',name,str(defs)))

        process_block = r"""def process(msg):
    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    
    # end custom process definition
    
    return api.Message(attributes={'name':'concat','type':'str'},body=None),log_stream.getvalue()
    """
        operatorFile.write("\n\n" + process_block + "\n")

        operatorFile.writelines("inports = {}\n".format(str(operatorj['inports'])))
        operatorFile.writelines("outports = {}\n".format(str(operatorj['outports'])))

        last1block = r"""def call_on_input(msg) :
    new_msg, log = process(msg)
    api.send(outports[0]['name'],new_msg)
    api.send(outports[1]['name'],log)
    """
        operatorFile.write("\n" + last1block + "\n")
        if len(operatorj['outports']) > 1 :
            iplist = list()
            for i in range(0,len(operatorj['outports'])) :
                iplist.append("inports[{}]['name']".format(i))
            setpcallback = "api.set_port_callback({}), call_on_input)".format(str(iplist))
        else :
            setpcallback = "api.set_port_callback(inports[0]['name'], call_on_input)"
        operatorFile.write('#'+setpcallback+'\n\n')

        last2block = r"""def main() :
    print('Test: Default')
    """
        operatorFile.write(last2block)
        operatorFile.write(setpcallback+'\n\n')

        last3block = r"""if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        """
        operatorFile.write(last3block + '\n')
        operatorFile.close()

###############################################################################################################
### MAIN
###############################################################################################################

def main() :

    logging.Formatter('%(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Generate SAP Data Intelligence solution for local operator code')

    parser.add_argument('--project',help='<new project path> - creates new project with required folder structure')
    parser.add_argument('--version', help='<version> in format <num.num.num>')
    parser.add_argument('--debug',action='store_true', help='For debug-level information ')
    parser.add_argument('--zip',action='store_true', help='Zipping solution folder ')
    parser.add_argument('--force', action='store_true', help='Removes subdirectories from <solution/operators>')
    parser.add_argument('--reverse',action='store_true',help='Ceates a custom operator script from solution package')
    parser.add_argument('--package', help='<package name> for reverse custom operator creation')
    parser.add_argument('--operator', help='<package.operator folder> for reverse custom operator creation')

    args = parser.parse_args()
    # testing args
    #args = parser.parse_args(['--project', '../newproject','--force'])
    #args = parser.parse_args(['--version', '0.0.3','--debug','--force'])
    args = parser.parse_args(['--reverse','--debug','--package','pandasOperators-0.0.16','--operator','pandas.toCSV'])

    version = args.version
    debug = args.debug
    zip_flag = args.zip
    clear_solution_path = args.force
    reverse = args.reverse
    package = args.package
    operator_folder = args.operator


    if debug :
        logging.basicConfig(level=logging.DEBUG,format='%(levelname)s - %(message)s')
        logging.debug('Logging Level: DEBUG')
    else :
        logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s')

    ###############################################################################################################
    ### makes project directories
    ###############################################################################################################
    if args.project:
        projpathdir = os.path.dirname(args.project)
        projpath = args.project
        projname = os.path.basename(args.project)
        if not os.path.isdir(projpathdir) :
            logging.error('Path to new project does not exist:  ' + projpathdir)
            exit(-1)
        logging.info('Prepares folder structure at <{}> for project <{}>'.format(projpath,projname))

        os.mkdir(projpath)
        src_path = os.path.join(projpath, 'src')
        logging.info('Creates sdi_utils-directory: ' + src_path)
        os.mkdir(src_path)
        src_project_path = os.path.join(src_path,projname)
        logging.info('Creates package-directory: ' + src_project_path)
        os.mkdir(src_project_path)
        solution_path = os.path.join(projpath, 'solution')
        logging.info('Creates solution-directory: ' + solution_path)
        os.mkdir(solution_path)
        logging.info('Creates README.md')
        readme_file = os.path.join(projpath,'README.md')
        with open(readme_file, 'w') as rmfile:
            readme = '# ' + projname + '\n\nDescription'
            rmfile.write(readme)

        # download templateCode.py
        template_url = 'https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py'
        download_templatefile(template_url,os.path.join(src_project_path,"customOperatorTemplate.py"))
        exit(1)

    if version and not re.match(r'\d+\.\d+\.\d+', version) :
        logging.error('<version> does not match required format <d.d.d> :' + version)
        exit(-1)

    ###############################################################################################################
    ### build solution
    ###############################################################################################################
    if not args.reverse :
        project_path = os.getcwd()                              # root path of the whole project
        src_path = os.path.join(project_path,'src')             # path of the src
        solution_path = os.path.join(project_path, "solution", "operators")

        ### clear solution folder to avoid ambiguities
        if os.path.isdir(solution_path)  and os.listdir(solution_path) :
            logging.warning('Solution path <solution/operators/> not empty')
            if clear_solution_path :
                logging.info('Remove subdirectory <solution/operators>')
                rmtree(solution_path, ignore_errors=False, onerror=None)
            else :
                logging.error('Either clear directory <solutions/operators/> manually or run with option --clear')
                exit(-1)

        ### generate the json files and copy them to solution directory
        for root, dirs, files in os.walk(src_path):
            for d in dirs:
                if d in exclude_dirs :
                    continue
            for f in files :
                if f in exclude_files :
                    continue
                if re.match(r'.+\.py$',f) :
                    logging.info ('Build files of : {} in {}'.format(f,root))
                    module_path = os.path.join(root[len(project_path) + 1:],f)[:-3]
                    module = module_path.replace(os.path.sep,'.')
                    logging.debug('Module: ' + module)
                    m = __import__(module,fromlist=module)
                    gensolution(os.path.join(root,f),config = m.api.config,inports = m.inports,outports=m.outports)

        ###  creating operator solutions for uploading
        if zip_flag :
            for d in os.listdir(solution_path):
                if d in exclude_dirs or re.match(r'.+.zip',d) :  # Zips are interpreted as directories
                    continue
                source_dir = os.path.join(solution_path, d)
                logging.debug('Building solution of folder: ' + source_dir)
                if version :
                    logging.debug('Change version in <manifest.json> file')
                    change_version(os.path.join(solution_path,d,'manifest.json'),version = version)
                    d = re.sub(r'(\d+\.\d+\.\d+)$',version,d)
                    dest_dir = os.path.join(solution_path,d)
                    logging.info('Rename folder: {} -> {}'.format(source_dir,dest_dir))
                    move(source_dir, dest_dir)
                    source_dir = os.path.join(solution_path, d)
                tarfilename = os.path.join(solution_path, d + '.zip')
                logging.info('Start vctl cmd: vctl solution bundle {} -t {}'.format(source_dir, tarfilename))
                subprocess.run(["vctl", "solution", "bundle", source_dir, "-t", tarfilename])

    ###############################################################################################################
    ### reverse solution
    ###############################################################################################################
    if args.reverse :
        project_path = os.path.dirname(os.getcwd())
        reverse_solution(project_path,package=package,operator_folder=operator_folder)

if __name__ == '__main__':
    main()
