# This script will get the updated config files for the mentioned jobs and create/update their events and targets in PROD Environemnt.
# If the job is already present, it will first store the current config in "Rollback" and then modify their configuration.

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
    config = data['CloudWatchConfigNPROD']

cloudwatch_events = boto3.client('events',region_name=config['region_name'])
client = boto3.client('lambda',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

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

def add_lambda_permission(event_name, event_arn, lambda_arn):
    try:
        event_arn = event_arn.replace(config['DEV_Acc'],config['PROD_Acc'])
        response = client.add_permission(
            FunctionName=lambda_arn,
            StatementId=event_name+'_Event_Trigger',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=event_arn
        )
    except client.exceptions.ResourceConflictException as e:
        print('For Rule:',event_name,'Permission was already there for ARN:',lambda_arn)
    except Exception as e:
        print('ERROR:',e)

def create_cw(cw_job,event_name): #This function creates/updates the events and targets
    if 'EventPattern' in cw_job:
        reponse = cloudwatch_events.put_rule(
            Name = cw_job['Name'],
            EventPattern = cw_job['EventPattern'],
            State = cw_job['State']
        )
    else:
        reponse = cloudwatch_events.put_rule(
            Name = cw_job['Name'],
            ScheduleExpression = cw_job['ScheduleExpression'],
            State = cw_job['State']
        )
    with open(path_prefix+config['Output_targets_prefix']+event_name+'.json','r') as f:
        targ = json.load(f)
    r = cloudwatch_events.list_targets_by_rule(
        Rule = cw_job['Name']
    )
    targets = r['Targets']
    if len(targets)>0:
        for t in targets:
            res = cloudwatch_events.remove_targets(
                Rule = cw_job['Name'],
                Ids = [t['Id']]
            )
    resp = cloudwatch_events.put_targets(
        Rule = cw_job['Name'],
        Targets = targ
    )
    print('Target : ',targ)
    print('CloudWatch Event and Target created : ',event_name)
    for target in targ:
        if 'Arn' in target:
            if 'lambda' in target['Arn']:
                add_lambda_permission(cw_job['Name'],cw_job['Arn'],target['Arn'])

for index in range(size):
    event_name = df['CW'][index] #getting job name from .csv file
    with open(path_prefix+config['Output_event_prefix']+event_name+'.json','r') as f: #reading event file from the Git
        cw_job = json.load(f)
    try:
        resp_event = cloudwatch_events.describe_rule( #checking if the job is already there in PROD or not
            Name = cw_job['Name']
        )
        #store_rollback(config['Rollback_event_prefix']+config['rollback_version'],df['CW'][index],resp_event)
        with open(path_prefix+config['Rollback_event_prefix']+df['CW'][index]+'-'+str(df['PR_ID'][index])+'.json', 'w',encoding = 'UTF-8') as outfile: # store the current config in Rollback if job is present
            json.dump(resp_event, outfile)
        resp_targets = cloudwatch_events.list_targets_by_rule(
            Rule = cw_job['Name']
        )
        targets = resp_targets['Targets']
        #store_rollback(config['Rollback_target_prefix']+config['rollback_version'],df['CW'][index],targets)
        with open(path_prefix+config['Rollback_target_prefix']+df['CW'][index]+'-'+str(df['PR_ID'][index])+'.json', 'w',encoding = 'UTF-8') as outfile:
            json.dump(targets, outfile)
        create_cw(cw_job,event_name)
    except cloudwatch_events.exceptions.ResourceNotFoundException as e: #if job is not found, it will create a new job
        create_cw(cw_job,event_name)