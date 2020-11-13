# This script will take the updated config and zipped code as input and will create/update the lambda functions.

import sys
import boto3
import time
import json
import re
import pandas
from zipfile import ZipFile
from time import sleep
import shutil
import os

with open('../config_NPROD.json','r') as f:
    data = json.load(f)
    config = data['LambdaConfigNPROD']

client = boto3.client('lambda',region_name=config['region_name'])

df = pandas.read_csv('../Lambda/'+config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')
path_prefix = sys.argv[1]

# def store_rollback(dirpath,jobname,config):
    # try:
        # os.mkdir(dirpath+jobname)
        # filename = jobname+'-1.json'
        # f = open(dirpath+jobname+'/'+filename,'w')
        # json.dump(config,f)
        # f.close()
    # except FileExistsError as e:
        # list_files = os.listdir(dirpath+jobname)
        # number_files = len(list_files)
        # filename = jobname+'-'+str(number_files+1)+'.json'
        # f = open(dirpath+jobname+'/'+filename,'w')
        # json.dump(config,f)
        # f.close()

def add_event_invoke(func_name,retryattempts,maxage):
    res_invoke = client.put_function_event_invoke_config(
        FunctionName = func_name,
        MaximumRetryAttempts = retryattempts,
        MaximumEventAgeInSeconds = maxage
    )
    print('Modified asynchronous invocation for function: ',func_name)

def update_lambda(config_file,zip_code,py_file,pr_id): # This function will update the lambda function config as well as code in PROD Environment.
    res_lambda = client.get_function(
        FunctionName = config_file['FunctionName']
    )
    func_config = res_lambda['Configuration'] # Before updation, the current config is stored as 'Rollback' in Git.
    #store_rollback(config['Rollback_prefix']+config['rollback_version'],config_file['FunctionName'],func_config)
    with open(path_prefix+config['Rollback_prefix']+config_file['FunctionName']+'-'+str(pr_id)+'.json', 'w',encoding = 'UTF-8') as outfile:
        json.dump(func_config, outfile)
    if "VpcConfig" in config_file.keys():
        layers_arns = []
        if 'Layers' in config_file:
            for layer in config_file['Layers']:
                layers_arns.append(layer['Arn'])
        response = client.update_function_configuration(
            FunctionName = config_file['FunctionName'],
            Runtime = config_file['Runtime'],
            Role = config_file['Role'],
            Handler = py_file+'.lambda_handler',
            Timeout = config_file['Timeout'],
            MemorySize = config_file['MemorySize'],
            VpcConfig ={
                'SubnetIds':config_file['VpcConfig']['SubnetIds'],
                'SecurityGroupIds':config_file['VpcConfig']['SecurityGroupIds']}
        )
        res_lambda_code = client.update_function_code(
            FunctionName = config_file['FunctionName'],
            ZipFile = zip_code
        )
        
    else:
        if 'Environment' in config_file:
            res_lambda_conf = client.update_function_configuration(
                FunctionName = config_file['FunctionName'],
                Role = config_file['Role'],
                Handler = py_file+'.lambda_handler',
                Timeout = config_file['Timeout'],
                Environment = config_file['Environment'],
                MemorySize = config_file['MemorySize']
            )
            res_lambda_code = client.update_function_code(
                FunctionName = config_file['FunctionName'],
                ZipFile = zip_code
            )
        else:
            res_lambda_conf = client.update_function_configuration(
                FunctionName = config_file['FunctionName'],
                Role = config_file['Role'],
                Handler = py_file+'.lambda_handler',
                Timeout = config_file['Timeout'],
                MemorySize = config_file['MemorySize']
            )
            res_lambda_code = client.update_function_code(
                FunctionName = config_file['FunctionName'],
                ZipFile = zip_code
            )

size = len(df)
for index in range(size):
    job = df['Lambda'][index]
    pr_id = df['PR_ID'][index]
    file = open(path_prefix+config['Zip_Code_prefix']+df['Lambda'][index]+'.zip','rb') # Gets the Zipped code from Git.
    zip_code = file.read()
    py_file_list = config['Python_file_name']
    py_file = py_file_list.split('.')[0] # Gets the python file handler. By default its 'lambda_function'.
    with open(path_prefix+config['Config_prefix']+job+'.json','r') as f: # Gets the updated function config from Git.
        config_file = json.load(f)
    if 'daasbatchextract' in config_file['Role']:
        config_file['Role'] = config['RoleArn']
    else:
        config_file['Role'] = config['RoleArn']
    with open(path_prefix+config['Config_output_prefix']+df['Lambda'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Stores the updated function config in 'Output' folder.
        json.dump(config_file, outfile)
    try:
        if "VpcConfig" in config_file.keys():
            layers_arns = []
            if 'Layers' in config_file:
                for layer in config_file['Layers']:
                    layers_arns.append(layer['Arn'])
            response = client.create_function(
                FunctionName = config_file['FunctionName'],
                Runtime = config_file['Runtime'],
                Role = config_file['Role'],
                Handler = py_file+'.lambda_handler',
                Code = {
                    'ZipFile':zip_code
                    },
                Timeout = config_file['Timeout'],
                MemorySize = config_file['MemorySize'],
                VpcConfig ={
                    'SubnetIds':config_file['VpcConfig']['SubnetIds'],
                    'SecurityGroupIds':config_file['VpcConfig']['SecurityGroupIds']}
            )
        else:
            key = "Environment" # Checks if 'Environment' is present in keys or not.
            if key in config_file.keys():
                response = client.create_function(
                FunctionName = config_file['FunctionName'],
                Runtime = config_file['Runtime'],
                Role = config_file['Role'],
                Handler = py_file+'.lambda_handler',
                Code = {
                    'ZipFile':zip_code
                    },
                Timeout = config_file['Timeout'],
                MemorySize = config_file['MemorySize'],
                Environment = config_file['Environment']
                )
            else: # It is called when Environment Variables are not present.
                response = client.create_function(
                    FunctionName = config_file['FunctionName'],
                    Runtime = config_file['Runtime'],
                    Role = config_file['Role'],
                    Handler = py_file+'.lambda_handler',
                    Code = {
                        'ZipFile':zip_code
                        },
                    Timeout = config_file['Timeout'],
                    MemorySize = config_file['MemorySize']
                )
        print('Function Created : ',job)
        if "MaximumEventAgeInSeconds" in config_file.keys():
            add_event_invoke(config_file['FunctionName'],config_file['MaximumRetryAttempts'],config_file['MaximumEventAgeInSeconds'])
    except client.exceptions.ResourceConflictException as e:
        print(config_file['FunctionName']+" : Function already exists, Updating the function")
        update_lambda(config_file,zip_code,py_file,pr_id)
        if "MaximumEventAgeInSeconds" in config_file.keys():
            add_event_invoke(config_file['FunctionName'],config_file['MaximumRetryAttempts'],config_file['MaximumEventAgeInSeconds'])