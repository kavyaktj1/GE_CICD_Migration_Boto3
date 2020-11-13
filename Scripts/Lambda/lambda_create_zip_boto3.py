# This script will take the config and code for functions mentioned in the job list as input. It will zip the code file with necessary permissions and store it in Git.
# The config file will also be stored in the Git.

import sys
import boto3
import time
import json
import re
import pandas
from zipfile import ZipInfo,ZipFile,ZIP_DEFLATED
from time import sleep
import shutil
import os

with open('../config1.json','r') as f:
    data = json.load(f)
    config = data['LambdaConfigPROD']

client = boto3.client('lambda',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')
path_prefix = sys.argv[1]
size = len(df)
for index in range(size):
    with open(path_prefix+config['JSON_File_DEV']+df['Lambda'][index]+'.json','r') as json_file: # Gets the config file for the job.
        func_config = json.load(json_file)
    with open(path_prefix+config['python_File_DEV_path']+df['Lambda'][index]+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
        python_code = f.read()
        with open(path_prefix+config['python_File_PROD']+df['Lambda'][index]+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
            outfile.write(python_code)
    shutil.copy(path_prefix+config['Git_script_location_prefix']+df['Lambda'][index]+".py",config['Python_file_name']) # Moves and renames the python code file as 'lambda_function.py'
    file = open(config['Python_file_name'],'rb')
    zip_code = file.read()
    zip_file_name = path_prefix+config['Zip_Code_prefix']+df['Lambda'][index]+'.zip' # Creates the zip file and add the permissions.
    zip_file = ZipFile(zip_file_name, 'w', compression=ZIP_DEFLATED)
    zip_info = ZipInfo(config['Python_file_name'])
    zip_info.compress_type = ZIP_DEFLATED
    zip_info.create_system = 3
    zip_info.external_attr = 0o777 << 16
    zip_file.writestr(zip_info,zip_code) # Adds the code to the zip file.
    os.remove(config['Python_file_name'])
    if 'Environment' in func_config:
        env = func_config['Environment']
        variables = env['Variables']
        if 'LAMBDA_ENV' in variables:
            variables['LAMBDA_ENV'] = 'PROD'
        if 'envprefix' in variables:
            variables['envprefix'] = 'us-prod-odp'
        if 'BUCKET_NAME' in variables:
            variables['BUCKET_NAME'] = 'odp-us-prod-raw'
        env['Variables'] = variables
        func_config['Environment'] = env
    with open(path_prefix+config['Config_prefix']+df['Lambda'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Store the config for function in Git.
        json.dump(func_config, outfile)
