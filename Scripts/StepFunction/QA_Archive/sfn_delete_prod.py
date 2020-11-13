# This script will take the SFN names as input and will delete those SFN's in PROD Environment.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['StepFunctionConfigPROD']

client = boto3.client('stepfunctions',region_name=config['region_name'])

df = pandas.read_csv(config['delete_job_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

def delete_job(var): # This function will take the job name as argument and will delete that job from PROD Environment.
    response = client.delete_state_machine(
        stateMachineArn=var
    )
    print("SFN : "+var+" deleted")

for index in range(size):
    response = client.list_executions( # Gets the executions for the SFN.
        stateMachineArn = config['sfn_job_arn_prefix']+df['SFN'][index]
    )
    if (len(response['executions'])==0): # If it is not yet executed, we can call 'delete_job' function.
        delete_job(config['sfn_job_arn_prefix']+df['SFN'][index])
    else:
        execut = response['executions'][0]
        status = execut['status']
        if status!='RUNNING': # If it is not in 'RUNNING' state, we can call the 'delete_job' function.
            delete_job(config['sfn_job_arn_prefix']+df['SFN'][index])
        else:
            print("SFN : "+df['SFN'][index]+" is running. It cannot be deleted")
