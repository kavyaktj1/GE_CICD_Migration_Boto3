# This script will get the SNS Topic and subscription config from Git and will create/update the SNS in QA Environment.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep
import os

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['SNSConfigQA']

client = boto3.client('sns',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

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

def get_name(topicarn):
    topicarn_split_info = topicarn.split(':')
    return topicarn_split_info[-1]
    
def rollback_sns_job(input_topic_name,pr_id): # This function will store the current config for the mentioned SNS Topic as Rollback.
    response = client.list_topics(
    )
    topics = response['Topics']
    for topic in topics:
        topic_name = get_name(topic['TopicArn'])
        if topic_name==input_topic_name:
            resp = client.list_subscriptions_by_topic(
                TopicArn = topic['TopicArn']
            )
            subs = resp['Subscriptions']
            topic_dict = {}
            topic_dict[topic_name] = topic['TopicArn']
            topic_dict['Subscriptions'] = subs
            #store_rollback(config['Rollback_prefix']+config['rollback_version'],topic_name,topic_dict)
            with open(config['Rollback_prefix']+topic_name+'-'+pr_id+'.json', 'w',encoding = 'UTF-8') as outfile:
                json.dump(topic_dict, outfile)

for index in range(size):
    pr_id = df['PR_ID'][index]
    with open(config['Git_File_Prefix']+df['SNS'][index]+'.json','r') as f: # Gets the SNS config from Git.
        sns = json.load(f)
    with open(config['Input_File_Prefix']+df['SNS'][index]+'.json','w',encoding = 'UTF-8') as f: # Stores the initial config in 'Input' folder in Git.
        json.dump(sns,f)
    with open(config['Output_File_Prefix']+df['SNS'][index]+'.json','w',encoding = 'UTF-8') as f: # Stores the final config in 'Input' folder in Git.
        json.dump(sns,f)
    rollback_sns_job(df['SNS'][index],pr_id) # It will store the current config as 'Rollback' in Git.
    response = client.create_topic( # It will create/Update the Topic.
        Name = df['SNS'][index],
    )
    Topicarn = response['TopicArn']
    subs = sns['Subscriptions']
    for sub in subs:
        resp = client.subscribe( # It will create/update the subscription config for a particular Topic.
            TopicArn = Topicarn,
            Protocol = sub['Protocol'],
            Endpoint = sub['Endpoint']
        )


