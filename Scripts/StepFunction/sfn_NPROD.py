# This script will get the config as well as definition from Git. It will then modify the definition parameters according to the requirements
# and create/update the SFN in PROD.

import sys
import boto3
import time
import json
import re
import os
import pandas

with open('../config_NPROD.json','r') as f:
    data = json.load(f)
    config = data['StepFunctionConfigNPROD']

sfnClient =  boto3.client('stepfunctions',region_name=config['region_name'])
client = boto3.client('logs',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)
path_prefix = sys.argv[1]
def get_pr_id(jobname,pr_id):
    path = config['JSON_File_Git']
    files_pr = []
    pr_id = int(pr_id)
    for i in os.listdir(path):
        if os.path.isfile(os.path.join(path,i)) and jobname in i:
            file_list = i.split('.')
            prid = int(file_list[0].split('-')[-1])
            print(prid)
            files_pr.append(prid)
    max_pr = 0
    for pr in files_pr:
        if pr<pr_id and pr>max_pr:
            max_pr = pr
    return max_pr

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


def create_log_group(jobname): # It will create the log groups for the SFN with name as input parameter.
    try:
        group = client.create_log_group(
            logGroupName = config['cw_log_group_prefix']+jobname+config['cw_log_group_postfix']
        )
    except client.exceptions.ResourceAlreadyExistsException as e: # If log group with same name is already there, it will skip.
        pass

def modify_params(key,value): # This function will modify the parameters the parameters based on requirement.
    params = config['Parameter_Modify']
    for param in params:
        for para_key in param:
            if key==para_key:
                if len(param[para_key])==1:
                    return True,param[para_key][0]
                else:
                    value = value.replace(param[para_key][0],param[para_key][1])
                    return True,value
    return False,None

def load_params(obj): # This will load all the parameters from the definition recursively and pass it onto 'modify_params'.
    keys = obj.keys()
    for key in keys:
        if isinstance(obj[key], list):
            for item in obj[key]:
                if type(item)!=str:
                  load_params(item)

        if isinstance(obj[key],dict):
            load_params(obj[key])
        if type(obj[key])==str:
            flag,value = modify_params(key,obj[key])
            if flag :
                obj[key] = value


def create_state_machine(jobname,pr_id): # This function will create the SFN with function name as argument.
    try:
        with open(path_prefix+config['JSON_File_Prefix']+jobname+'.json','r') as json_file: # Gets the SFN Definition from the Git.
            def_data = json.load(json_file)
        with open(path_prefix+config['JSON_File_DEV_Git']+jobname+'.json', 'w',encoding = 'UTF-8') as outfile: # Stores the initial config in 'Input' Folder.
            json.dump(def_data, outfile)
        load_params(def_data)
        updated_definition = json.dumps(def_data, indent = 4) # Gets the updated definition.
        create_log_group(jobname)
        response = sfnClient.create_state_machine(
            name = jobname,
            definition = updated_definition,
            roleArn = config['RoleArn'],
            loggingConfiguration = {
                'level':'ALL',
                'includeExecutionData':True,
                'destinations':[
                    {
                        'cloudWatchLogsLogGroup':{
                            'logGroupArn':config['cw_log_group_arn_prefix']+config['cw_log_group_prefix']+jobname+config['cw_log_group_postfix']+':*'
                        }
                    },
                ]
            }
        )
        with open(path_prefix+config['JSON_File_Git']+jobname+'-'+pr_id+'.json', 'w',encoding = 'UTF-8') as outfile: # Stores the final config in 'Output' Folder.
            json.dump(updated_definition, outfile)
    except Exception as e:
        print('Getting exception while SFN creation for : ',jobname)
    
def modify_state_machine(job,name,pr_id): # This function will modify the SFN with function name and config as arguments.
    try:
        with open(path_prefix+config['JSON_File_Prefix']+name+'.json','r') as json_file: # Gets the SFN Definition from the Git.
            def_data = json.load(json_file)
        with open(path_prefix+config['JSON_File_DEV_Git']+name+'.json', 'w',encoding = 'UTF-8') as outfile: # Stores the initial config in 'Input' Folder.
            json.dump(def_data, outfile)
        load_params(def_data)
        updated_definition = json.dumps(def_data, indent = 4) # Gets the updated definition.
        create_log_group(name)
        response = sfnClient.update_state_machine(
            stateMachineArn = job,
            definition = updated_definition,
            roleArn = config['RoleArn'],
            loggingConfiguration = {
                'level':'ALL',
                'includeExecutionData':True,
                'destinations':[
                    {
                        'cloudWatchLogsLogGroup':{
                            'logGroupArn':config['cw_log_group_arn_prefix']+config['cw_log_group_prefix']+name+config['cw_log_group_postfix']+':*'
                        }
                    },
                ]
            }
        )
        with open(path_prefix+config['JSON_File_Git']+name+'-'+pr_id+'.json', 'w',encoding = 'UTF-8') as outfile: # Stores the final config in 'Output' Folder.
            json.dump(updated_definition, outfile) 
    except Exception as e:
        print('Getting exception while updating function : ',name)

for index in range(size):
    job = df['SFN'][index]
    print(job,' is getting executed')
    job = job.strip()
    try:
        response = sfnClient.describe_state_machine( # Checks if the SFN is already there or not.
            stateMachineArn = config['sfn_job_arn_prefix']+job
        )
        response['creationDate'] = str(response['creationDate']) # Stores the current config as 'Rollback' in Git.
        #store_rollback(config['Rollback_Path']+config['rollback_version'],response['name'],response)
        with open(path_prefix+config['Rollback_Path']+response['name']+'-'+str(df['PR_ID'][index])+'.json', 'w',encoding = 'UTF-8') as outfile:
            json.dump(response,outfile)
        modify_state_machine(config['sfn_job_arn_prefix']+job,job,str(df['PR_ID'][index]))
    except sfnClient.exceptions.StateMachineDoesNotExist as e: # If SFN is not found in Console, create_state_machine is called.
        create_state_machine(job,str(df['PR_ID'][index]))
