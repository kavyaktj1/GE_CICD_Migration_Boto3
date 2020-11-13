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

exec_time_ignore_list = ['arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_MASTER_ONHAND_SCHEDULE','arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_SPOTFIRE_REFRESH_ONHAND']

flag_Mapp_flat = False
flag_Mapp_glprod = False

df = pandas.read_csv('Scripts/SFN_Details_MAPP_Onhand.csv')
df = df.dropna(axis = 0, how = 'any')

size = len(df)
tz = pytz.timezone('Asia/Kolkata')
now = dt.now().astimezone(tz)
dt_string = now.strftime("%d-%m-%Y %H %M")

def get_same_day_exec(execution_list):
    final_list = []
    for execution in execution_list:
        start_date = execution['startDate']
        starttime_ind = start_date.astimezone(tz)
        today_date = dt.today().astimezone(tz).replace(hour=1, minute=15)
        difference = starttime_ind-today_date
        if difference.days==0:
            final_list.append(execution)
    return final_list

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

def get_latest_run(SFN_Name):
    response = sfnClient.list_executions(
        stateMachineArn=SFN_Name
    )
    executions = response['executions']
    if SFN_Name not in exec_time_ignore_list:
        executions = check_dummy_execution(executions)
    final_executions = get_same_day_exec(executions)
    func_startdate = ''
    func_enddate = ''
    func_exectime = ''
    func_status = ''
    if len(final_executions)==0:
        func_startdate = '-'
        func_enddate = '-'
        func_exectime = '-'
        func_status = 'WAITING'
    for latest_execution in final_executions:
        starttimefordiff = latest_execution['startDate']
        starttime_ind = starttimefordiff.astimezone(tz)
        today_date = dt.today().astimezone(tz).replace(hour=1, minute=15)
        difference = starttime_ind-today_date
        if difference.days!=0:
            latest_execution['status'] = 'WAITING'
        if latest_execution['status']=='WAITING':
            func_startdate = func_startdate+'-<br />'
        else:
            func_startdate = func_startdate+starttime_ind.strftime("%b %d, %Y, %H:%M")+'<br />'
        if 'stopDate' in latest_execution.keys() and latest_execution['status']!='WAITING':
            stopdate_ind = latest_execution['stopDate'].astimezone(tz)
            func_enddate = func_enddate+stopdate_ind.strftime("%b %d, %Y, %H:%M")+'<br />'
            endtimefordiff = latest_execution['stopDate']
            timediff = endtimefordiff - starttimefordiff
            minutes = divmod(timediff.seconds, 60)
            if minutes[0]>=60:
                hours = int(minutes[0]/60)
                mins = minutes[0]%60
                func_exectime = func_exectime+str(hours)+' hrs, '+str(mins)+' mins, '+str(minutes[1])+' secs'+'<br />'
            else:
                func_exectime = func_exectime+str(minutes[0])+' mins, '+str(minutes[1])+' secs'+'<br />'
        else:
            func_enddate = func_enddate+'-<br />'
            func_exectime = func_exectime+'-<br />'
        func_status = func_status+latest_execution['status']+'<br />'
    status.append(func_status)
    startDates.append(func_startdate)
    endDates.append(func_enddate)
    execTime.append(func_exectime)
    

for index in range(size):
    SFN_Name = df['SFN'][index]
    get_latest_run('arn:aws:states:us-east-1:245792935030:stateMachine:'+SFN_Name)

df['Status'] = status
df['Start Time'] = startDates
df['End Time'] = endDates
df['Execution Time'] = execTime

print('Sequence No.\tType\tState Machine Name\tStatus\tStart Time\tEnd Time\tExecution Time\n')
countint = 0

df.to_csv('Scripts/SFN_Status_Archive/ODP_MAPP_Onhand_Load_Status '+dt_string+'.csv', index=False, encoding='utf-8')

for index in range(size):
    SFN_Name = df['SFN'][index]
    Type = df['Type'][index]
    status1 = df['Status'][index]
    starttime = df['Start Time'][index]
    endtime = df['End Time'][index]
    exectim = df['Execution Time'][index]
    countint+=1
    print(str(countint)+'.\t'+Type+'\t'+SFN_Name+'\t'+status1+'\t'+starttime+'\t'+endtime+'\t'+exectim+'\n')
