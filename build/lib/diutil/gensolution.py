
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

def gensolution(script_path, config, inports, outports, src_path=None, project_path = None, override_readme = False, tar = False) :

    if not project_path :
        project_path = os.getcwd()

    if not src_path :
        src_path = os.path.join(project_path,"diutil")

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
    # * remove import gensolution - maybe used in same script for generating
    with open(script_path, 'r') as read_fn:
        with open(os.path.join(operator_solution_path, script_filename), 'w') as write_fn:
            line = read_fn.readline()
            while line :
                line = re.sub(r'^#api.set_port_callback', 'api.set_port_callback', line)
                if re.match(r'if __name__ == \'__main__\'',line) or re.match(r'if __name__ == \"__main__\"',line) :
                    break
                if re.match(r'import gensolution',line) :
                    line = read_fn.readline()
                    continue
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



def main() :

    logging.Formatter('%(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Generate SAP Data Intelligence solution for local operator code')

    parser.add_argument('--project',help='Creates new project with folder structure for locally programming operators ')
    parser.add_argument('--version', help='version format <num.num.num>')
    parser.add_argument('--debug',action='store_true', help='For debug-level information ')
    parser.add_argument('--zip',action='store_true', help='Zipping solution folder ')
    parser.add_argument('--force', action='store_true', help='Removes subdirectories from <solution/operators>')

    args = parser.parse_args()
    # testing args
    #args = parser.parse_args(['--project', '../newproject','--force'])
    #args = parser.parse_args(['--version', '0.0.3','--debug','--force'])


    version = args.version
    debug = args.debug
    zip_flag = args.zip
    clear_solution_path = args.force

    if debug :
        logging.basicConfig(level=logging.DEBUG,format='%(levelname)s - %(message)s')
        logging.debug('Logging Level: DEBUG')
    else :
        logging.basicConfig(level=logging.INFO,format='%(levelname)s - %(message)s')


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
        logging.info('Creates diutil-directory: ' + src_path)
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


    project_path = os.getcwd()
    src_path = os.path.join(project_path,'diutil')
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


if __name__ == '__main__':
    main()
