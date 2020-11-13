# This script will get the Crawler config from Git for DEV Environment and store an initial config in 'Input' folder and modify the config according
# to QA Environment and store the updated config in 'Output' folder in Git.

import sys
import boto3
import time
import json
import re
import pandas

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['CrawlerConfigQA']

glueClient = boto3.client('glue',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')
size = len(df)

if size==0:
    print('No Crawlers to Migrate')
    sys.exit(0)

for index in range(size):
    with open(config['JSON_File_Prefix'] + df['Crawlers'][index] + '.json', 'r') as f: # Getting the crawler config from Git.
        crw = json.load(f)
    with open(config['Input_Git_Prefix']+df['Crawlers'][index] + '.json', 'w', encoding='UTF-8') as outfile: # Storing the initial config in 'Input' folder.
        json.dump(crw, outfile)
    crw['Name'] = config['name_prefix'] + crw['Name'] # Modifying the name of crawler
    crw['CreationTime'] = str(crw['CreationTime'])
    crw['LastUpdated'] = str(crw['LastUpdated'])
    obj = crw['LastCrawl']
    obj['StartTime'] = str(obj['StartTime'])
    crw['LastCrawl'] = obj
    with open(config['Output_Git_Prefix']+df['Crawlers'][index] + '.json', 'w', encoding='UTF-8') as outfile: # Storing the final config in 'Output' folder.
        json.dump(crw, outfile)