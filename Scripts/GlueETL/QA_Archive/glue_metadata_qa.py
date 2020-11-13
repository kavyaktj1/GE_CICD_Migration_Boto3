# This script will get the Glue Job config for DEV Environment from Git and modify the configuration according to QA Environment and store the updated config
# in 'Output' Folder.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['GlueETLConfigQA']

glue = boto3.client('glue',region_name=config['region_name'])
s3 = boto3.client('s3')

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')


def change_default_arguments(defarg): # Modifies the default arguments in the DEV config file according to PROD requirements
    keys = defarg.keys()
    for key in keys:
        if(key.find('--aws_iam_role')!=-1):
            defarg['--aws_iam_role'] = config['--aws_iam_role']
    defarg['--TempDir'] = config['--TempDir']
    defarg['--job-bookmark-option'] = config['--job-bookmark-option']
    defarg['--extra-py-files'] = config['--extra-py-files']
    return defarg

def change_script_loc(command,track,filename): # Changes the Track and prefix in script location path.
    script_loc = command['ScriptLocation']
    newloc = config['Script_location_prefix']+track+'/src/GlueETL/'+filename+'.py'
    command['ScriptLocation'] = newloc
    return command

size = len(df)

for index in range(size):
    job = df['JobName'][index]
    track = df['Track'][index]
    job = job.strip()
    with open(config['JSON_File_DEV']+track+'/'+job+'.json','r') as f: # Getting the config file from Git.
        glue_job = json.load(f)
        with open(config['JSON_Git_Input']+job+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
            json.dump(glue_job, outfile)
        time.sleep(0.5)
        glue_job['Role'] = config['--aws_iam_role'] # Chnaging the role of job.
        glue_job['Command'] = change_script_loc(glue_job['Command'],df['Track'][index],glue_job['Name']) # Changing the script location of the job.
        glue_job['Name'] = glue_job['Name'] # Modifying the name of the job by adding prefix.
        glue_job['CreatedOn'] = str(glue_job['CreatedOn'])
        glue_job['LastModifiedOn'] = str(glue_job['LastModifiedOn'])
        if 'WorkerType' not in glue_job.keys():
            glue_job['WorkerType'] = 'Standard'
            glue_job['NumberOfWorkers'] = 4
        glue_job['DefaultArguments'] = change_default_arguments(glue_job['DefaultArguments']) # Modifying the default arguments according to QA.
        glue_job['Tags'] = {'Track':df['Track'][index]} # Adding Tags in form of Track which is retreived from file list.
        with open(config['JSON_File_QA']+job+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the final config in 'Output' folder.
            json.dump(glue_job, outfile, indent=4)

