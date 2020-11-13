# The script will retrieve the updated QA Config from Git and create or update the crawler in QA Environment.

import sys
import boto3
import time
import json
import re
import os
import pandas

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['CrawlerConfigQA']

glueClient = boto3.client('glue',region_name=config['region_name'])

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

def create_crawler(cr):
    new_crawler = glueClient.create_crawler(
        Name=cr['Name'],
        Role=cr['Role'],
        DatabaseName=cr['DatabaseName'],
        Targets=cr['Targets'],
        SchemaChangePolicy=cr['SchemaChangePolicy']
    )

def modify_crawler(cr):
    new_crawler = glueClient.update_crawler(
        Name=cr['Name'],
        Role=cr['Role'],
        DatabaseName=cr['DatabaseName'],
        Targets=cr['Targets'],
        SchemaChangePolicy=cr['SchemaChangePolicy']
    )

if size==0:
    sys.exit(0)

for index in range(size):
    with open(config['Output_Git_Prefix']+df['Crawlers'][index]+'.json','r') as f: # Getting the updated crawler config from 'Output' folder.
        cr = json.load(f)
    try:
        response = glueClient.get_crawler( # Checking if crawler is already there or not.
            Name=cr['Name']
        )
        crw = response['Crawler']
        crw['CreationTime'] = str(crw['CreationTime'])
        crw['LastUpdated'] = str(crw['LastUpdated'])
        obj = crw['LastCrawl']
        obj['StartTime'] = str(obj['StartTime'])
        crw['LastCrawl'] = obj
        #store_rollback(config['Rollback_Prefix']+config['rollback_version'],df['Crawlers'][index],crw)
        with open(config['Rollback_Prefix'] + df['Crawlers'][index]+'-'+str(df['PR_ID'][index])+ '.json', 'w', encoding='UTF-8') as outfile: # If crawler is present, current config is stored in rollback
            json.dump(crw, outfile)
        modify_crawler(cr) # Modify crawler is called
    except glueClient.exceptions.EntityNotFoundException as e:
        create_crawler(cr) # Crawler is not present, so new crawler is created.
    


