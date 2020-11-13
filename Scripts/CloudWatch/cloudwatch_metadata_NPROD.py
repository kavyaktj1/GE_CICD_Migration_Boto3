# This script will get the event and target config from Git and store an initial config in 'Input' folder and modify the config according
# to PROD Environment and store the updated config in 'Output' folder in Git.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config_NPROD.json','r') as f:
    data = json.load(f)
    config = data['CloudWatchConfigNPROD']

cloudwatch_events = boto3.client('events',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

path_prefix = sys.argv[1]
def modify_targets(targ):
    try:
        targ_input = targ['Input']
        # targ_input = targ_input.replace(config['taget_modify_1_before'],config['taget_modify_1_after'])
        # targ_input = targ_input.replace(config['taget_modify_2_before'],config['taget_modify_2_after'])
        targ['Input'] = targ_input
    except Exception as e:
        print('Exception: ',e)
    return targ

size = len(df)
for index in range(size):  
    with open(path_prefix+config['Event_Git_Prefix']+df['CW'][index]+'.json','r') as f: # Reading the event config file from Git.
        resp_event = json.load(f)
    with open(path_prefix+config['Input_event_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the initial event config file in 'Input'.
        json.dump(resp_event, outfile)
    if 'EventPattern' in resp_event:
        resp_event['EventPattern'] = resp_event['EventPattern'].replace(config['DEV_Acc'],config['PROD_Acc'])
    if 'daasbatchextract' not in resp_event['Name']:
        resp_event['State'] = 'ENABLED'  
    #resp_event['Name'] = resp_event['Name'].replace(config['Event_name_before'],config['Event_name_after']) # Modify the name of the event.
    with open(path_prefix+config['Output_event_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the final event config file in 'Output'.
        json.dump(resp_event, outfile)

    with open(path_prefix+config['Target_Git_Prefix'] + df['CW'][index] + '.json', 'r') as f: # Reading the target config file from Git.
        targets = json.load(f)
    with open(path_prefix+config['Input_targets_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the initial target config file in 'Input'.
        json.dump(targets, outfile)
    for i in range(len(targets)): # Updating the target config
        targets[i]['Arn'] = targets[i]['Arn'].replace(config['Target_Arn_before'],config['Target_Arn_after'])
        name = targets[i]['Arn']
        targets[i]['Arn'] = name.replace(config['DEV_Acc'],config['PROD_Acc'])
        if 'RoleArn' in targets[i]:
            if 'daasbatchextract' in targets[i]['RoleArn']:
                targets[i]['RoleArn'] = 'arn:aws:iam::245792935030:role/odp-us-prod-daasbatchextract-service-role'
            else:
                targets[i]['RoleArn'] = config['roleArn']
        targets[i] = modify_targets(targets[i])
    with open(path_prefix+config['Output_targets_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the final target config file in 'Output'.
        json.dump(targets, outfile)
        print(targets)
