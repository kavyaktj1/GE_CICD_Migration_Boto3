# This script will get the event and target config from Git and store an initial config in 'Input' folder and modify the config according
# to QA Environment and store the updated config in 'Output' folder in Git.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['CloudWatchConfigQA']

cloudwatch_events = boto3.client('events',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)
for index in range(size):  
    with open(config['Event_Git_Prefix']+df['CW'][index]+'.json','r') as f: # Reading the event config file from Git.
        resp_event = json.load(f)
    with open(config['Input_event_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the initial event config file in 'Input'.
        json.dump(resp_event, outfile)
    resp_event['Name'] = resp_event['Name'] # Modify the name of the event.
    with open(config['Output_event_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the final event config file in 'Output'.
        json.dump(resp_event, outfile)

    with open(config['Target_Git_Prefix'] + df['CW'][index] + '.json', 'r') as f: # Reading the target config file from Git.
        targets = json.load(f)
    with open(config['Input_targets_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the initial target config file in 'Input'.
        json.dump(targets, outfile)
    with open(config['Output_targets_prefix']+df['CW'][index]+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the final target config file in 'Output'.
        json.dump(targets, outfile)
