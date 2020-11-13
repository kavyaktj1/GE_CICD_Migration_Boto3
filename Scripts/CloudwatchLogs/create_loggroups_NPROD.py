import boto3
import json
import pandas
import time
import sys
from datetime import datetime

def create_log_groups(loggroup):
    try:
        client = boto3.client('logs')
        response = client.create_log_group(
            logGroupName='/ODP-Assembly-Line/NPROD/'+loggroup
        )
        print('LogGroup : ',loggroup,' created')
    except client.exceptions.ResourceAlreadyExistsException as e:
        print('LogGroup : ',loggroup,' already exists')

if __name__ == '__main__':
	list_of_loggroups = ['AWS-Cloudwatch','AWS-DynamoDB','AWS-Crawler','AWS-Glue','AWS-Glue-Classifier','AWS-Lambda','AWS-SNS','AWS-StepFunction','AWS-Redshift-DDL','AWS-Redshift-SP','CSV-File-Creation','Master']
	for loggroup in list_of_loggroups:
		create_log_groups(loggroup)