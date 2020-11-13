import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['LambdaConfigPROD']

client = boto3.client('lambda',region_name=config['region_name'])

df = pandas.read_csv(config['delete_job_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

for index in range(size):
    try:
        response = client.delete_function(
            FunctionName=df['Lambda'][index]
        )
        print("Lambda Function : "+df['Lambda'][index]+" deleted")
    except clients.exceptions.ResourceConflictException as e:
        print("Lambda Function : "+df['Lambda'][index]+" is running. It cannot be deleted")
    except client.exceptions.ResourceNotFoundException as f:
        print("Lambda Function : "+df['Lambda'][index]+" does not exist.")