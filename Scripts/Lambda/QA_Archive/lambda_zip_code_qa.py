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

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['LambdaConfigQA']

client = boto3.client('lambda',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)
for index in range(size):
    with open(config['JSON_File_DEV']+df['Lambda'][index]+'.json','r') as json_file: # Gets the config file for the job.
        func_config = json.load(json_file)
    with open(config['python_File_DEV_path']+df['Lambda'][index]+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
        python_code = f.read()
        with open(config['python_File_QA']+df['Lambda'][index]+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
            outfile.write(python_code)
    shutil.move(config['Git_script_location_prefix']+df['Lambda'][index]+".py",config['Python_file_name']) # Moves and renames the python code file as 'lambda_function.py'
    file = open(config['Python_file_name'],'rb')
    zip_code = file.read()
    zip_file_name = config['Zip_Code_prefix']+df['Lambda'][index]+'.zip' # Creates the zip file and add the permissions.
    zip_file = ZipFile(zip_file_name, 'w', compression=ZIP_DEFLATED)
    zip_info = ZipInfo(config['Python_file_name'])
    zip_info.compress_type = ZIP_DEFLATED
    zip_info.create_system = 3
    zip_info.external_attr = 0o777 << 16
    zip_file.writestr(zip_info,zip_code) # Adds the code to the zip file.
    os.remove(config['Python_file_name'])
    with open(config['Config_prefix']+df['Lambda'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Store the config for function in Git.
        json.dump(func_config, outfile)
