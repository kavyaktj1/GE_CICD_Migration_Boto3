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

def create_sns_topic(file_name):
    sns_client = boto3.client('sns',region_name='us-east-1')
    with open('SNS/'+file_name+'.json','r') as f: # Gets the SNS config from Git.
        sns = json.load(f)
    response = sns_client.create_topic( # It will create/Update the Topic.
        Name = file_name,
    )
    print('Topic Created : ',file_name)
    Topicarn = response['TopicArn']
    subs = sns['Subscriptions']
    for sub in subs:
        resp = sns_client.subscribe( # It will create/update the subscription config for a particular Topic.
            TopicArn = Topicarn,
            Protocol = sub['Protocol'],
            Endpoint = sub['Endpoint']
        )
    print('Subscribers added for topic : ',file_name)
    
def create_glue_jobs(glue_job):
    glue = boto3.client('glue',region_name='us-east-1')
    try:
        response = glue.get_job( # Checks whether the job is already there or not.
            JobName = glue_job
        )
        response = glue.update_job(
            JobName = glue_job,
            JobUpdate ={
                'Role':'arn:aws:iam::341594139623:role/odp-us-nprod-servicesuite-service-role',
                'ExecutionProperty':{"MaxConcurrentRuns": 1},
                'Command':{"Name": "pythonshell","ScriptLocation": "s3://odp-us-nprod-servicesuite/common/502818085/ODP_CICD_Migration/Scripts/NPROD_Initialize/GlueETL/Code/"+glue_job+".py","PythonVersion": "3"},
                'Connections':{"Connections": ["us-nprod-peering-redshift"]},
                'MaxRetries':0,
                'MaxCapacity':0.0625,
                'GlueVersion':"1.0",
                #'NumberOfWorkers':glue_job['NumberOfWorkers'],
                #'WorkerType':glue_job['WorkerType'],
                'Timeout':2880
            }
        )
        print(glue_job + " Modified")
    except glue.exceptions.EntityNotFoundException as e:
        response = glue.create_job(
            Name = glue_job,
            Role = 'arn:aws:iam::341594139623:role/odp-us-nprod-servicesuite-service-role',
            ExecutionProperty = {"MaxConcurrentRuns": 1},
            Command = {"Name": "pythonshell","ScriptLocation": "s3://odp-us-nprod-servicesuite/common/502818085/ODP_CICD_Migration/Scripts/NPROD_Initialize/GlueETL/Code/"+glue_job+".py","PythonVersion": "3"},
            Connections = {"Connections": ["us-nprod-peering-redshift"]},
            MaxRetries = 0,
            MaxCapacity = 0.0625,
            GlueVersion = "1.0",
            #NumberOfWorkers = glue_job['NumberOfWorkers'],
            #WorkerType = glue_job['WorkerType'],
            Timeout = 2880
        )
        print(glue_job + " Created")

if __name__ == '__main__':
    list_of_loggroups = ['AWS-Cloudwatch','AWS-DynamoDB','AWS-Crawler','AWS-Glue','AWS-Glue-Classifier','AWS-Lambda','AWS-SNS','AWS-StepFunction','AWS-Redshift-DDL','AWS-Redshift-SP','CSV-File-Creation','Master']
    print('Migrating Initial LogGroups for Cloudwatch Logging -')
    for loggroup in list_of_loggroups:
        create_log_groups(loggroup)
    #sns_files = ['us-odp-nprod-dev2prod-merge4deploy-failure','us-odp-nprod-dev2prod-merge4deploy-success','us-odp-nprod-devops-assembly-line-failure','us-odp-nprod-devops-assembly-line-success']
    # print('Migrating Initial SNS Topics -')
    # for sns in sns_files:
        # create_sns_topic(sns)
    print('Migrating Glue Jobs for Redshift DDL and SP Migration -')
    glue_jobs = ['ODP_CICD_Migrate_Redshift_DDL','ODP_CICD_Migrate_Redshift_SP']
    for job in glue_jobs:
        create_glue_jobs(job)
