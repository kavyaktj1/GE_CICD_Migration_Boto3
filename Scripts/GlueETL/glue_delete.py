# This script will delete the Glue Jobs mentioned in the job file list. The script is same for both QA and PROD Environments.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['GlueETLConfigPROD']

glue = boto3.client('glue',region_name=config['region_name'])

df = pandas.read_csv(config['delete_job_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

def delete_job(var): # This function will delete the job mentioned in the argument.
    response = glue.delete_job(
        JobName = var
    )
    print("Job : "+var+" deleted.")

for index in range(size):
    try:
        response = glue.get_job_runs( # Getting the job executions from console for the given job.
            JobName = df['JobName'][index],
            MaxResults = 1
        )
        time.sleep(0.5)
        if (len(response['JobRuns'])==0): # If the job is not yet executed, we can call 'delete_job' function.
            delete_job(df['JobName'][index])
        else:
            state = response['JobRuns'][0]['JobRunState']
            if state!='RUNNING': # If the job is not in running state, we can call 'delete_job' function.
                delete_job(df['JobName'][index])
    except glue.exceptions.EntityNotFoundException as e:
        print("Job : "+df['JobName'][index]+" not found.")
    except Exception as e:
        print ('ERROR at ',index, ':',e)
    