# This script will get the config as well as definition from Git. It will then modify the definition parameters according to the requirements
# and create/update the SFN in PROD.

import sys
import boto3
import time
import json
import re
import os
import pandas
import pytz
from datetime import datetime as dt
import datetime

sfnClient =  boto3.client('stepfunctions',region_name='us-east-1')
startDates = []
endDates = []
status = []
execTime = []

exec_time_ignore_list = ['arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_REFRESH_SPOTFIRE','arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_RS_AUDIT_FINISH']

flag_Mapp_flat = False
flag_Mapp_glprod = False

df = pandas.read_csv('Scripts/SFN_Details.csv')
df = df.dropna(axis = 0, how = 'any')

tz = pytz.timezone('Asia/Kolkata')
cd_entity_start = dt.now().astimezone(tz)
cd_entity_flag = 'ACTIVE'
Compaction_end_max = 'WAITING'
size = len(df)
now = dt.now().astimezone(tz)
dt_string = now.strftime("%d-%m-%Y %H %M")

def check_dummy_execution(execution_list):
    for execution in execution_list:
        start_date = execution['startDate']
        starttime_ind = start_date.astimezone(tz)
        today_date = dt.today().astimezone(tz).replace(hour=1, minute=15)
        difference = starttime_ind-today_date
        if difference.days==0:
            if execution['status']=='SUCCEEDED':
                stop_date = execution['stopDate']
                exec_time = stop_date-start_date
                if exec_time.seconds<60:
                    execution_list.remove(execution)
    return execution_list

def get_latest_run(SFN_Name,SFN_type):
    global flag_Mapp_flat,flag_Mapp_glprod,Compaction_end_max,cd_entity_start,cd_entity_flag
    response = sfnClient.list_executions(
        stateMachineArn=SFN_Name
    )
    executions = response['executions']
    if SFN_Name not in exec_time_ignore_list:
        executions = check_dummy_execution(executions)
    latest_execution = executions[0]
    starttimefordiff = latest_execution['startDate']
    starttime_ind = starttimefordiff.astimezone(tz)
    if SFN_Name=='arn:aws:states:us-east-1:245792935030:stateMachine:CD_ENTITY_MASTER_SCHEDULE':
        cd_entity_start = starttime_ind
    today_date = dt.today().astimezone(tz).replace(hour=1, minute=15)
    difference = starttime_ind-today_date
    if difference.days!=0:
        if SFN_Name=='arn:aws:states:us-east-1:245792935030:stateMachine:CD_ENTITY_MASTER_SCHEDULE':
            cd_entity_flag = 'WAITING'
        latest_execution['status'] = 'WAITING'
    elif SFN_type=='ENTITY' or SFN_type=='CDS':
        if 'MAPP' not in SFN_Name:
            if starttime_ind<cd_entity_start or cd_entity_flag=='WAITING':
                latest_execution['status'] = 'WAITING'
    if SFN_Name=='arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_FLAT_FILE_COMPACTION':
        if latest_execution['status']=='WAITING' or latest_execution['status']=='RUNNING':
            flag_Mapp_flat = True
    elif SFN_Name=='arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_GLPROD_COMPACTION':
        if latest_execution['status']=='WAITING' or latest_execution['status']=='RUNNING':
            flag_Mapp_glprod = True
    if latest_execution['status']=='WAITING':
        startDates.append('-')
    else:
        startDates.append(starttime_ind.strftime("%b %d, %Y, %H:%M"))
    if 'stopDate' in latest_execution.keys() and latest_execution['status']!='WAITING':
        stopdate_ind = latest_execution['stopDate'].astimezone(tz)
        if Compaction_end_max=='WAITING':
            if SFN_type=='COMPACTION':
                Compaction_end_max = stopdate_ind
        else:
            if stopdate_ind>Compaction_end_max:
                if SFN_type=='COMPACTION':
                    Compaction_end_max = stopdate_ind
        endDates.append(stopdate_ind.strftime("%b %d, %Y, %H:%M"))
        endtimefordiff = latest_execution['stopDate']
        timediff = endtimefordiff - starttimefordiff
        minutes = divmod(timediff.seconds, 60)
        if minutes[0]>=60:
            hours = int(minutes[0]/60)
            mins = minutes[0]%60
            execTime.append(str(hours)+' hrs, '+str(mins)+' mins, '+str(minutes[1])+' secs')
        else:
            execTime.append(str(minutes[0])+' mins, '+str(minutes[1])+' secs')
    else:
        endDates.append('-')
        execTime.append('-')
    status.append(latest_execution['status'])

for index in range(size):
    SFN_Name = df['SFN'][index]
    SFN_type = df['Type'][index]
    get_latest_run('arn:aws:states:us-east-1:245792935030:stateMachine:'+SFN_Name,SFN_type)

df['Status'] = status
df['Start Time'] = startDates
df['End Time'] = endDates
df['Execution Time'] = execTime

if flag_Mapp_flat==True:
    for index in range(size):
        df['Status'][index] = 'WAITING'
        df['Start Time'][index] = '-'
        df['End Time'][index] = '-'
        df['Execution Time'][index] = '-'
elif flag_Mapp_glprod==True:
    for index in range(size):
        if df['SFN'][index]!='MAPP_FLAT_FILE_COMPACTION':
            df['Status'][index] = 'WAITING'
            df['Start Time'][index] = '-'
            df['End Time'][index] = '-'
            df['Execution Time'][index] = '-'

print('Sequence No.\tType\tState Machine Name\tStatus\tStart Time\tEnd Time\tExecution Time\n')
countint = 0
df.to_csv('Scripts/SFN_Status_Archive/ODP_Load_Status '+dt_string+'.csv', index=False, encoding='utf-8')

for index in range(size):
    SFN_Name = df['SFN'][index]
    Type = df['Type'][index]
    status1 = df['Status'][index]
    starttime = df['Start Time'][index]
    endtime = df['End Time'][index]
    exectim = df['Execution Time'][index]
    countint+=1
    print(str(countint)+'.\t'+Type+'\t'+SFN_Name+'\t'+status1+'\t'+starttime+'\t'+endtime+'\t'+exectim+'\n')
