# The script will retrieve the updated PROD Config from Git and create or update the Glue Job in PROD Environment.

import sys
import boto3
import time
import json
import re
import pandas
import os
from time import sleep

with open('../config_NPROD.json','r') as f:
    data = json.load(f)
    config = data['GlueETLConfigNPROD']

glue = boto3.client('glue',region_name=config['region_name'])
path_prefix = sys.argv[1]
# Needs to be 'DEV','NPROD' or 'PROD'
ENVIRONMENT = 'NPROD'

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

def create_new_job(glue_job,track): # This function will take job config as argument and will create the respective job.
    response = glue.create_job(
        Name = glue_job['Name'],
        Role = glue_job['Role'],
        ExecutionProperty = glue_job['ExecutionProperty'],
        Command = glue_job['Command'],
        DefaultArguments = glue_job['DefaultArguments'],
        Connections = glue_job['Connections'],
        MaxRetries = glue_job['MaxRetries'],
        MaxCapacity = glue_job['MaxCapacity'],
        GlueVersion = glue_job['GlueVersion'],
        #NumberOfWorkers = glue_job['NumberOfWorkers'],
        #WorkerType = glue_job['WorkerType'],
        Timeout = glue_job['Timeout']
    )
    print(glue_job['Name'] + " Created")
    
def modify_job(glue_job,pr_id,track): # This function takes job config as argument and modifies the respective job.
    global path_prefix
    resp = glue.get_job(
        JobName = glue_job['Name']
    )
    job = resp['Job']
    job['CreatedOn'] = str(job['CreatedOn'])
    job['LastModifiedOn'] = str(job['LastModifiedOn'])
    #store_rollback(config['Rollback_Path']+config['rollback_version'],job['Name'],job)
    with open(path_prefix+config['Rollback_Path']+job['Name']+'-'+pr_id+'.json', 'w',encoding = 'UTF-8') as outfile: # Before modification, current config is stored in Git as 'Rollback'.
       json.dump(job,outfile)
    response = glue.update_job(
        JobName = glue_job['Name'],
        JobUpdate ={
            'Role':glue_job['Role'],
            'ExecutionProperty':glue_job['ExecutionProperty'],
            'Command':glue_job['Command'],
            'DefaultArguments':glue_job['DefaultArguments'],
            'Connections':glue_job['Connections'],
            'MaxRetries':glue_job['MaxRetries'],
            'MaxCapacity':glue_job['MaxCapacity'],
            'GlueVersion':glue_job['GlueVersion'],
            #'NumberOfWorkers':glue_job['NumberOfWorkers'],
            #'WorkerType':glue_job['WorkerType'],
            'Timeout':glue_job['Timeout']
        }
    )
    print(glue_job['Name'] + " Modified")
    
def check_job_status(glue_job,pr_id,track): # This function will check the current job status. If the job is not in 'RUNNING' state, it will call the 'update_job' function.
    try:
        response = glue.get_job_runs(
            JobName = glue_job['Name'],
            MaxResults = 1
        )
        time.sleep(0.5)
        if (len(response['JobRuns'])==0):
            modify_job(glue_job,pr_id,track)
        else:
            state = response['JobRuns'][0]['JobRunState']
            if state!='RUNNING':
                modify_job(glue_job,pr_id,track)
    except glue.exceptions.EntityNotFoundException as e:
        pass
    except Exception as e:
        print ('ERROR :',e)


df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

for index in range(size):
    job = df['JobName'][index]
    track = df['Track'][index]
    pr_id = str(df['PR_ID'][index])
    with open(path_prefix+config['JSON_File_PROD']+job+'.json') as f: # Gets the updated file config for PROD Environment from Git.
        JOB = json.load(f)
        try:
            response = glue.get_job( # Checks whether the job is already there or not.
                JobName = JOB['Name']
            )
            time.sleep(0.5)
            check_job_status(JOB,pr_id,track)
        except glue.exceptions.EntityNotFoundException as e:
            create_new_job(JOB,track)
        except Exception as e:
            print ('ERROR at ',index, ':',e)
