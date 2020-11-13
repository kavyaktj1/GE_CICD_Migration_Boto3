# This script will delete the events and targets of the jobs mentioned in the file list. The same script can be used for both QA and PROD Environment.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['CloudWatchConfigPROD']

cloudwatch_events = boto3.client('events',region_name=config['region_name'])

df = pandas.read_csv(config['delete_job_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

for index in range(size):
	try:
		r = cloudwatch_events.list_targets_by_rule( # Getting the targets for the job
			Rule = df['CloudWatch'][index]
		)
		tar = r['Targets']
		if len(tar)>0: # Removing all the targets for the event
			for t in tar:
				res = cloudwatch_events.remove_targets(
					Rule = df['CloudWatch'][index],
					Ids = [t['Id']]
				)
		response = client.delete_rule( # Deleting the event
			Name = df['CloudWatch'][index]
		)
	except cloudwatch_events.exceptions.ResourceNotFoundException as e:
		print("CloudWatch Rule : "+df['CloudWatch'][index]+" not found.")