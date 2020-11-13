import boto3
import json
import pandas
import time
import sys
from datetime import datetime

def create_log_stream(log_group,log_stream):
	try:
		response = client.create_log_stream(
			logGroupName=log_group,
			logStreamName=log_stream
		)
	except Exception as e:
		print('ERROR : ',e,' while creating log stream')
		
def publish_message_to_stream(log_group,log_stream,log_message):
	try:
		response = client.describe_log_streams(
			logGroupName=log_group,
			logStreamNamePrefix=log_stream
		)
		event_log = {
			'logGroupName': log_group,
			'logStreamName': log_stream,
			'logEvents': [
				{
					'timestamp': int(round(time.time() * 1000)),
					'message': log_message
				}
			],
		}
		#if 'uploadSequenceToken' in response['logStreams'][0]:
		#	event_log.update({'sequenceToken': response['logStreams'][0] ['uploadSequenceToken']})
		response = client.put_log_events(**event_log)
	except Exception as e:
		print('ERROR : ',e,' while publishing message to log stream')

if __name__ == '__main__':
    try:
        client = boto3.client('logs',region_name='us-east-1')
        component_name = sys.argv[1].strip()
        pr_id = sys.argv[2].strip()
        curr_time = datetime.now()
        time_stamp = curr_time.strftime("%d-%m-%Y %H-%M")
        log_group = '/ODP-Assembly-Line/NPROD/'+component_name
        log_stream = 'PR ID - '+pr_id+' Time - '+time_stamp
        create_log_stream(log_group,log_stream)
        updated_message = ''
        log_message = open('console_log.log', encoding='utf-8').readlines()
        for message in log_message:
            if '8mha:////' in message:
                continue
            else:
                updated_message = updated_message+message+'\n'
        publish_message_to_stream(log_group,log_stream,updated_message)
    except Exception as e:
        print('ERROR : ',e,' in the main function')