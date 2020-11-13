# This script will delete the crawlers mentioned in the job file list. The script is same for both QA and PROD Environments.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['CrawlerConfigPROD']

glue = boto3.client('glue',region_name=config['region_name'])

df = pandas.read_csv(config['delete_job_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

for index in range(size):
	try:
		response = glue.delete_crawler( # Get the crawler name and call 'delete_crawler' function.
			Name=df['Crawler'][index]
		)
		print("Deleted Crawler : "+df['Crawler'][index])
	except glue.exceptions.CrawlerRunningException as e: # Crawler won't be deleted if it is already running.
		print(df['Crawler'][index]+" is running. It cannot be deleted")
	except glue.exceptions.EntityNotFoundException as f:
		print("Crawler : "+df['Crawler'][index]+" does not exist")