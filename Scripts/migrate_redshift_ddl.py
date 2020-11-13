import sys
import boto3
import time
import json
import re
import pandas
import os
from time import sleep

glue = boto3.client('glue',region_name='us-east-1')

def log_search(job_run_id):
    logs = boto3.client('logs',region_name='us-east-1')
    #log_response = logs.filter_log_events(logGroupName='/aws-glue/python-jobs/output',logStreamNames=[job_run_id])
    log_response= logs.get_log_events (logGroupName='/aws-glue/python-jobs/output',logStreamName=job_run_id)
    #print (log_response)
    for event in log_response['events']:
        message=event["message"]
        print(message)
        mess_split = message.splitlines()
        last_line = mess_split[-1]
        if 'FAILURE' in last_line:
            raise Exception('Some DDL scripts failed')

response = glue.start_job_run(
	JobName='migrate_redshift_ddl_partial_rollback'
)
print('Job Triggered')
time.sleep(3)

response1 = glue.get_job_run(
    JobName = 'migrate_redshift_ddl_partial_rollback',
    RunId=response['JobRunId']
)
print('Found Job Run')
state = response1['JobRun']['JobRunState']
ti = 0
while state=='RUNNING':
    time.sleep(5)
    ti+=5
    print('Job is in running state for ',ti,' seconds')
    response1 = glue.get_job_run(
        JobName = 'migrate_redshift_ddl_partial_rollback',
        RunId=response['JobRunId']
    )
    state = response1['JobRun']['JobRunState']
else:
    log_search(response['JobRunId'])
