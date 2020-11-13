# The script will retrieve the updated PROD Config from Git and create or update the Glue Job in PROD Environment.

import sys
import boto3
import time
import json
import re
import pandas
import os
from time import sleep

with open('../config1.json','r') as f:
    data = json.load(f)
    config = data['GlueETLConfigPROD']

glue = boto3.client('glue',region_name=config['region_name'])
path_prefix = sys.argv[1]
# Needs to be 'DEV' or 'PROD'
ENVIRONMENT = 'PROD'

df = pandas.read_csv(config['classifier_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

def modify_classifier(job_data):
    if 'CsvClassifier' in job_data:
        dict_data = job_data['CsvClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.update_classifier(
            CsvClassifier=dict_data
        )
    elif 'JsonClassifier' in job_data:
        dict_data = job_data['JsonClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.update_classifier(
            JsonClassifier=dict_data
        )
    elif 'XMLClassifier' in job_data:
        dict_data = job_data['XMLClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.update_classifier(
            XMLClassifier=dict_data
        )
    elif 'GrokClassifier' in job_data:
        dict_data = job_data['GrokClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.update_classifier(
            GrokClassifier=dict_data
        )
    print("Classifier : ",job_data," modified")

def create_new_classifier(job_data):
    if 'CsvClassifier' in job_data:
        dict_data = job_data['CsvClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.create_classifier(
            CsvClassifier=dict_data
        )
    elif 'JsonClassifier' in job_data:
        dict_data = job_data['JsonClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.create_classifier(
            JsonClassifier=dict_data
        )
    elif 'XMLClassifier' in job_data:
        dict_data = job_data['XMLClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.create_classifier(
            XMLClassifier=dict_data
        )
    elif 'GrokClassifier' in job_data:
        dict_data = job_data['GrokClassifier']
        if 'CreationTime' in dict_data:
            del dict_data['CreationTime']
        if 'LastUpdated' in dict_data:
            del dict_data['LastUpdated']
        if 'Version' in dict_data:
            del dict_data['Version']
        response = glue.create_classifier(
            GrokClassifier=dict_data
        )
    print("Classifier : ",job_data," created")

for index in range(size):
    name = df['Name'][index]
    with open(path_prefix+config['classifier_File_PROD']+name+'.json') as f: # Gets the updated file config for PROD Environment from Git.
        JOB = json.load(f)
        try:
            response = glue.get_classifier( # Checks whether the job is already there or not.
                Name = name
            )
            time.sleep(0.5)
            modify_classifier(JOB)
        except glue.exceptions.EntityNotFoundException as e:
            create_new_classifier(JOB)
        except Exception as e:
            print('ERROR at ',index, ':',e)