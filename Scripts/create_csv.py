import sys
import boto3
import time
import json
import re
import pandas
import os
import csv
from time import sleep

df = pandas.read_csv('job_list.csv')
df = df.dropna(axis = 0, how = 'any')

size = len(df)

glue_list_qa = []
glue_list_prod = []
lambda_list_qa = []
lambda_list_prod = []
cw_list_qa = []
cw_list_prod = []
crawler_list_qa = []
crawler_list_prod = []
glueclassifier_qa = []
glueclassifier_prod = []
sfn_list_qa = []
sfn_list_prod = []
sns_list_qa = []
sns_list_prod = []
code_list = []
sp_list = []
ddl_list = []
dynamodb_prod = []

def write_to_csv(header,job_list_names,file_path):
    with open(file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(header)
        for path in job_list_names:
            writer.writerow(path)

def get_header(config_name):
    with open('config.json','r') as f:
        data = json.load(f)
        config = data[config_name]
        header = config['header']
    return header

def get_csv_path(config_name):
    with open('config.json','r') as f:
        data = json.load(f)
        config = data[config_name]
        csv_path = config['job_name_list']
    return csv_path
  
for index in range(size):
    job = []
    service = df['Service'][index]
    env = df['Environment'][index]
    job_name = df['JobName'][index]
    pr_id = str(df['PR_ID'][index])
    if service=='lambda':
        service = 'Lambda'
    elif service=='SFN':
        service = 'StepFunction'
    if(job_name.find('@#')!=-1):
        job = job_name.split('@#')
        job.append(pr_id)
    else:    
        job.append(job_name)
        job.append(pr_id)
    if service=='GlueETL':
        if env=='PROD':
            glue_list_prod.append(job)
        else:
            glue_list_qa.append(job)
    elif service=='Lambda':
        if env=='PROD':
            lambda_list_prod.append(job)
        else:
            lambda_list_qa.append(job)
    elif service=='StepFunction':
        if env=='PROD':
            sfn_list_prod.append(job)
        else:
            sfn_list_qa.append(job)
    elif service=='CloudWatch':
        if env=='PROD':
            cw_list_prod.append(job)
        else:
            cw_list_qa.append(job)
    elif service=='SNS':
        if env=='PROD':
            sns_list_prod.append(job)
        else:
            sns_list_qa.append(job)
    elif service=='GlueClassifier':
        if env=='PROD':
            glueclassifier_prod.append(job)
        else:
            glueclassifier_qa.append(job)
    elif service=='DynamoDB':
        job.append(env)
        dynamodb_prod.append(job)
    elif service=='Crawler':
        if env=='PROD':
            crawler_list_prod.append(job)
        else:
            crawler_list_qa.append(job)
    elif service=='LambdaCode' or service=='GlueCode':
        li = []
        li.append(service)
        li.append(env)
        li.append(job_name)
        li.append(pr_id)
        code_list.append(li)
    elif service=='sp' or service=='dbscript' or service=='view':
        li = []
        li.append(job_name)
        li.append(env)
        li.append(service)
        sp_list.append(li)
    elif service=='ddl':
        li = []
        li.append(job_name)
        li.append(env)
        li.append(service)
        li.append(pr_id)
        ddl_list.append(li)

glue_list_qa = [list(t) for t in set(tuple(element) for element in glue_list_qa)]
glue_list_prod = [list(t) for t in set(tuple(element) for element in glue_list_prod)]
lambda_list_qa = [list(t) for t in set(tuple(element) for element in lambda_list_qa)]
lambda_list_prod = [list(t) for t in set(tuple(element) for element in lambda_list_prod)]
cw_list_qa = [list(t) for t in set(tuple(element) for element in cw_list_qa)]
cw_list_prod = [list(t) for t in set(tuple(element) for element in cw_list_prod)]
crawler_list_qa = [list(t) for t in set(tuple(element) for element in crawler_list_qa)]
crawler_list_prod = [list(t) for t in set(tuple(element) for element in crawler_list_prod)]
sfn_list_qa = [list(t) for t in set(tuple(element) for element in sfn_list_qa)]
sfn_list_prod = [list(t) for t in set(tuple(element) for element in sfn_list_prod)]
sns_list_qa = [list(t) for t in set(tuple(element) for element in sns_list_qa)]
sns_list_prod = [list(t) for t in set(tuple(element) for element in sns_list_prod)]
code_list = [list(t) for t in set(tuple(element) for element in code_list)]
glueclassifier_prod = [list(t) for t in set(tuple(element) for element in glueclassifier_prod)]
glueclassifier_qa = [list(t) for t in set(tuple(element) for element in glueclassifier_qa)]

if len(glue_list_prod)>=0:
    service = 'GlueETL'
    env = 'PROD'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=['Track',header_name,'PR_ID']
    write_to_csv(header,glue_list_prod,csv_file_path)
    
if len(glue_list_qa)>=0:
    service = 'GlueETL'
    env = 'QA'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=['Track',header_name,'PR_ID']
    write_to_csv(header,glue_list_qa,csv_file_path)
    
if len(lambda_list_prod)>=0:
    service = 'Lambda'
    env = 'PROD'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,lambda_list_prod,csv_file_path)
    
if len(lambda_list_qa)>=0:
    service = 'Lambda'
    env = 'QA'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,lambda_list_qa,csv_file_path)
    
if len(crawler_list_prod)>=0:
    service = 'Crawler'
    env = 'PROD'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,crawler_list_prod,csv_file_path)
    
if len(glueclassifier_prod)>=0:
    service = 'GlueETL'
    csv_file_name = 'classifier_dev_prod.csv'
    csv_file_path = service+'/'+csv_file_name
    header_name = 'Name'
    header=[header_name,'PR_ID']
    write_to_csv(header,glueclassifier_prod,csv_file_path)
    
if len(dynamodb_prod)>=0:
    service = 'DynamoDB'
    csv_file_name = 'dynamo_dev_prod.csv'
    csv_file_path = service+'/'+csv_file_name
    header_name = 'FileName'
    header=[header_name,'PR_ID','Track']
    write_to_csv(header,dynamodb_prod,csv_file_path)
    
if len(crawler_list_qa)>=0:
    service = 'Crawler'
    env = 'QA'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,crawler_list_qa,csv_file_path)
    
if len(sfn_list_prod)>=0:
    service = 'StepFunction'
    env = 'PROD'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,sfn_list_prod,csv_file_path)
    
if len(sfn_list_qa)>=0:
    service = 'StepFunction'
    env = 'QA'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,sfn_list_qa,csv_file_path)
    
if len(sns_list_prod)>=0:
    service = 'SNS'
    env = 'PROD'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,sns_list_prod,csv_file_path)
    
if len(sns_list_qa)>=0:
    service = 'SNS'
    env = 'QA'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,sns_list_qa,csv_file_path)
    
    
if len(cw_list_prod)>=0:
    service = 'CloudWatch'
    env = 'PROD'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,cw_list_prod,csv_file_path)
    
if len(cw_list_qa)>=0:
    service = 'CloudWatch'
    env = 'QA'
    config_name = service+'Config'+env
    csv_file_name = get_csv_path(config_name)
    csv_file_path = service+'/'+csv_file_name
    header_name = get_header(config_name)
    header=[header_name,'PR_ID']
    write_to_csv(header,cw_list_qa,csv_file_path)
    
if len(code_list)>=0:
    csv_file_name = get_csv_path('Code')
    csv_file_path = 'Components_Code/'+csv_file_name
    header = ['Service','Track','JobName','PR_ID']
    write_to_csv(header,code_list,csv_file_path)

if len(sp_list)>=0:
    csv_file_name = 'redshift_sp.csv'
    csv_file_path = csv_file_name
    header = ['Name','Track','Type']
    write_to_csv(header,sp_list,csv_file_path)
    
if len(ddl_list)>=0:
    csv_file_name = 'redshift_ddl.csv'
    csv_file_path = csv_file_name
    header = ['Name','Track','Type','PR_ID']
    write_to_csv(header,ddl_list,csv_file_path)
    