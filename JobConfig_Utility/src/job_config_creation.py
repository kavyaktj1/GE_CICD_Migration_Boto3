# !/usr/bin/env python3
import boto3
import time
import json
import re
import pandas
from time import sleep
import argparse
import base64
import configparser
import getpass
import logging
import os
import socket
import sys
import urllib.parse
import urllib.request
from urllib.error import URLError, HTTPError
from xml.etree.ElementTree import XML
from datetime import datetime,date

today = date.today()
d1 = today.strftime("%d/%m/%Y")
now = datetime.now()
current_time_1 = now.strftime("%H:%M:%S")
current_time = d1+' '+current_time_1


with open('job_config.json','r') as f:
    config = json.load(f)

glue_path = config['glue_path'] #path for storing config files in local system
glue_job_list = config['glue_job_list'] #csv file containing the job list
glue_header = config['glue_header'] #header of csv file

crawler_path = config['crawler_path']
crawler_job_list = config['crawler_job_list']
crawler_header = config['crawler_header']

sns_path = config['sns_path']

cw_event_path = config['cw_event_path']
cw_target_path = config['cw_target_path']
cw_job_list = config['cw_job_list']
cw_header = config['cw_header']

lambda_header = config['lambda_header']
lambda_job_list = config['lambda_job_list']
lambda_path = config['lambda_path']

glue = boto3.client('glue', region_name='us-east-1')
sns_client = boto3.client('sns',region_name='us-east-1')
lambda_client = boto3.client('lambda',region_name='us-east-1')
cloudwatch_events = boto3.client('events',region_name='us-east-1')


def create_glue_file(var):
    try:
        response = glue.get_job(
            JobName=var
        )
        job = response['Job']
        job['CreatedOn'] = str(job['CreatedOn'])
        job['LastModifiedOn'] = str(job['LastModifiedOn'])
        with open(glue_path+var+'.json', 'w', encoding='UTF-8') as f:
            json.dump(job, f)
            print("Glue Config created : "+glue_path+var+'.json')
    except Exception as e:
        print("Exception",e)

def check_job(path,job):
    try:
        with open(path+job+'.json') as f:
            print(job + " Found")
            return True
    except FileNotFoundError as e:
        print("Config for "+job + " not found")
        return False

def glue_job():
    # Needs to be 'DEV' or 'PROD'
    ENVIRONMENT = 'DEV'
    df = pandas.read_csv(glue_job_list)
    df = df.dropna(axis=0, how='any')
    size = len(df)
    for index in range(size):
        job = df[glue_header][index]
        job = job.strip()
        if check_job(glue_path,job)==False:
            create_glue_file(job)
    os.system("pause")

def create_crawler_file(var):
    try:
        response = glue.get_crawler(
            Name = var
        )
        cr = response['Crawler']
        cr['CreationTime'] = str(cr['CreationTime'])
        cr['LastUpdated'] = str(cr['LastUpdated'])
        obj = cr['LastCrawl']
        obj['StartTime'] = str(obj['StartTime'])
        cr['LastCrawl'] = obj
        with open(crawler_path+var+'.json', 'w',encoding = 'UTF-8') as outfile:
            json.dump(cr, outfile)
            file = open('logs.txt','a+')
            file.write(current_time+' '+"Crawler Config created : "+crawler_path+var+'.json'+'\n')
            file.close()
            print("Crawler Config created : "+crawler_path+var+'.json')
    except Exception as e:
        file = open('logs.txt','a+')
        file.write(current_time+' '+e+'\n')
        file.close()

def crawler_job():
    df = pandas.read_csv(crawler_job_list)
    df = df.dropna(axis = 0, how = 'any')
    size = len(df)
    for index in range(size):
        job = df[crawler_header][index]
        job = job.strip()
        if check_job(crawler_path,job)==False:
            create_crawler_file(job)
    os.system("pause")

def get_name(topicarn):
    t_list = topicarn.split(':')
    return t_list[-1]
    
def sns_job():
    try:
        response = sns_client.list_topics(
        )
        topics = response['Topics']
        for topic in topics:
            t_name = get_name(topic['TopicArn'])
            resp = sns_client.list_subscriptions_by_topic(
                TopicArn = topic['TopicArn']
            )
            subs = resp['Subscriptions']
            topic_dict = {}
            topic_dict[t_name] = topic['TopicArn']
            topic_dict['Subscriptions'] = subs
            with open(sns_path+t_name+'.json', 'w',encoding = 'UTF-8') as outfile:
                json.dump(topic_dict, outfile)
                file = open('logs.txt','a+')
                file.write(current_time+' '+"SNS Config created : "+sns_path+t_name+'.json'+'\n')
                file.close()
                print("SNS Config created : "+sns_path+t_name+'.json')
        os.system("pause")
    except Exception as e:
        file = open('logs.txt','a+')
        file.write(current_time+' '+e+'\n')
        file.close()

def create_cw_file(var):
    try:
        resp = cloudwatch_events.describe_rule(
            Name = var
        )
        with open(cw_event_path+var+'.json', 'w',encoding = 'UTF-8') as outfile:
            json.dump(resp, outfile)
            file = open('logs.txt','a+')
            file.write(current_time+' '+"Created Event File: "+cw_event_path+var+'.json'+'\n')
            file.close()
            print("Created Event File: "+cw_event_path+var+'.json')
        response = cloudwatch_events.list_targets_by_rule(
            Rule = var
        )
        targets = response['Targets']
        with open(cw_target_path+var+'.json', 'w',encoding = 'UTF-8') as outfile:
            json.dump(targets, outfile)
            file = open('logs.txt','a+')
            file.write(current_time+' '+"Created Target File: "+cw_target_path+var+'.json'+'\n')
            file.close()
            print("Created Target File: "+cw_target_path+var+'.json')
    except Exception as e:
        file = open('logs.txt','a+')
        file.write(current_time+' '+e+'\n')
        file.close()

def cloudwatch_job():
    df = pandas.read_csv(cw_job_list)
    df = df.dropna(axis = 0, how = 'any')

    size = len(df)
    for index in range(size):
        job = df[cw_header][index]
        job = job.strip()
        if check_job(cw_event_path,job)==False or check_job(cw_target_path,job)==False:
            create_cw_file(job)
    os.system("pause")
			
def create_lambda_file(var):
    try:
        resp = lambda_client.get_function(
            FunctionName = var
        )
        func = resp['Configuration']
        with open(lambda_path+var+'.json', 'w',encoding = 'UTF-8') as outfile:
            json.dump(func, outfile)
            file = open('logs.txt','a+')
            file.write(current_time+' '+"Lambda Config : "+var+" created."+'\n')
            file.close()
            print("Lambda Config : "+var+" created.")
    except Exception as e:
        file = open('logs.txt','a+')
        file.write(current_time+' '+e+'\n')
        file.close()
        
def lambda_job():
    df = pandas.read_csv(lambda_job_list)
    df = df.dropna(axis = 0, how = 'any')
    size = len(df)
    for index in range(size):
        job = df[lambda_header][index]
        job = job.strip()
        if check_job(lambda_path,job)==False:
            create_lambda_file(job)
    os.system("pause")

if __name__ == '__main__':
    print("Enter the service you would like to use : ")
    print("1. GlueETL")
    print("2. Crawler")
    print("3. SNS")
    print("4. CloudWatch")
    print("5. Lambda")
    selection = int(input("Enter Selection Number : "))
    if selection==1:
        glue_job()
    elif selection==2:
        crawler_job()
    elif selection==3:
        sns_job()
    elif selection==4:
        cloudwatch_job()
    elif selection==5:
        lambda_job()
    else:
        print("Wrong Selection")
