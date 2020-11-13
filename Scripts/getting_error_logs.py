import boto3
import json
import pandas
import time
import sys
from datetime import datetime


def describe_log_streams(log_group):
    try:
        response = client.describe_log_streams(
            logGroupName=log_group,
            orderBy='LastEventTime',
            descending=True
        )
        latest_log_stream = response['logStreams'][0]['logStreamName']
        return latest_log_stream
    except Exception as e:
        print('ERROR : ',e,' while publishing message to log stream')

def get_log_events(log_group,log_stream):
    try:
        response = client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            startFromHead=True
        )
        messages = response['events']
        log_message = ''
        for message in messages:
            log_message+=log_message+message['message']
        return log_message
    except Exception as e:
        print('ERROR : ',e,' while fetching log messages')
        

if __name__ == '__main__':
    client = boto3.client('logs',region_name='us-east-1')
    component_name = sys.argv[1].strip()
    log_group = '/ODP-Assembly-Line/PROD/'+component_name
    log_stream = describe_log_streams(log_group)
    log_message = get_log_events(log_group,log_stream)
    print(log_message)