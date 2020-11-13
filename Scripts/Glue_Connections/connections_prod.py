# This script will retreive the Glue Connections config from PROD Environment and store it in Git.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['GlueConnections']

client = boto3.client('glue',region_name=config['region_name'])

response = client.get_connections(
)
connections = response['ConnectionList']
for conn in connections:
    conn_name = conn['Name']
    conn['CreationTime'] = str(conn['CreationTime'])
    conn['LastUpdatedTime'] = str(conn['LastUpdatedTime'])
    with open(config['path_prefix']+conn_name+'.json', 'w',encoding = 'UTF-8') as outfile:
        json.dump(conn, outfile)
