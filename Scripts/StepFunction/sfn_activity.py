# This script will get the config as well as definition from Git. It will then modify the definition parameters according to the requirements
# and create/update the SFN in PROD.

import sys
import boto3
import time
import json
import re
import os
import pandas

with open('../config1.json','r') as f:
    data = json.load(f)
    config = data['StepFunctionConfigPROD']

sfnClient =  boto3.client('stepfunctions',region_name=config['region_name'])
path_prefix = sys.argv[1]
df = pandas.read_csv(path_prefix+config['activity_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

for index in range(size):
    job = df['Activity'][index]
    job = job.strip()
    response = sfnClient.create_activity(
        name=job
    )
    print("Activity Created : "+job)