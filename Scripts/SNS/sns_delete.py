# This script will delete the SNS Topic and subscription config from console. The same script can be used for both QA and PROD Environment.

import sys
import boto3
import time
import json
import re
import pandas
import os
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['SNSConfigPROD']

client = boto3.client('sns',region_name=config['region_name'])

df = pandas.read_csv(config['delete_job_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

def get_name(topicarn):
    topicarn_split_info = topicarn.split(':')
    return topicarn_split_info[-1]

response = client.list_topics(
    )
topics = response['Topics']

for index in range(size):
    for topic in topics:
        topic_name = get_name(topic['TopicArn'])
        if topic_name==df['SNS'][index]:
            del_response = client.delete_topic(
                TopicArn=topic['TopicArn']
            )
            print("SNS Topic : "+topic_name+" is deleted")
