# This script will get the Crawler config from Git for DEV Environment and store an initial config in 'Input' folder and modify the config according
# to PROD Environment and store the updated config in 'Output' folder in Git.

import sys
import boto3
import time
import json
import re
import pandas

with open('../config1.json','r') as f:
    data = json.load(f)
    config = data['CrawlerConfigPROD']

def change_bucket_name(s3url, bucket_name):
    st = s3url[5:]
    st = st.split('/')
    st[0] = bucket_name
    new_s3_url = s3url[0:5]
    for s in st:
        new_s3_url+= s+'/'
    new_s3_url = new_s3_url[0:len(new_s3_url)-1]
    return new_s3_url


def update_target_paths(targets):
    targ = targets['S3Targets']
    for index in range(len(targ)):
        targ[index]['Path'] = change_bucket_name(targ[index]['Path'],config['new_bucket_name'])
    targets['S3Targets'] = targ
    return targets


glueClient = boto3.client('glue',region_name=config['region_name'])

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')
size = len(df)
path_prefix = sys.argv[1]

for index in range(size):
    with open(path_prefix+config['JSON_File_Prefix'] + df['Crawler'][index] + '.json', 'r') as f: # Getting the crawler config from Git.
        crw = json.load(f)
    with open(path_prefix+config['Input_Git_Prefix']+df['Crawler'][index] + '.json', 'w', encoding='UTF-8') as outfile: # Storing the initial config in 'Input' folder.
        json.dump(crw, outfile)
    #crw['Name'] = config['name_prefix'] + crw['Name'] # Modifying the name of crawler
    crw['Role'] = config['roleArn'] # Modifying the role of crawler
    crw['CreationTime'] = str(crw['CreationTime'])
    crw['LastUpdated'] = str(crw['LastUpdated'])
    if 'LastCrawl' in crw: 
        obj = crw['LastCrawl']
        obj['StartTime'] = str(obj['StartTime'])
        crw['LastCrawl'] = obj
    crw['Targets'] = update_target_paths(crw['Targets']) # Updating targets paths where bucket name is modified.
    with open(path_prefix+config['Output_Git_Prefix']+df['Crawler'][index] + '.json', 'w', encoding='UTF-8') as outfile: # Storing the final config in 'Output' folder.
        json.dump(crw, outfile)
