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

exec_time_ignore_list = ['arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_REFRESH_SPOTFIRE','arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_RS_AUDIT_FINISH','arn:aws:states:us-east-1:245792935030:stateMachine:ODP_DAILY_COMPACTION_SCHEDULE_START']

flag_Mapp_flat = False
flag_Mapp_glprod = False
last_run_flag = False

df = pandas.read_csv('Scripts/SFN_Details_MAPP_Daily.csv')
df = df.dropna(axis = 0, how = 'any')

size = len(df)
tz = pytz.timezone('Asia/Kolkata')
now = dt.now().astimezone(tz)
next_day = now+datetime.timedelta(days=1)
now_1125 = now.replace(hour=11, minute=25)
now_1925 = now.replace(hour=19, minute=25)
now_0325 = next_day.replace(hour=0, minute=5)
dt_string = now.strftime("%d-%m-%Y %H %M")

if now>now_1125 and now<now_1925:
    last_run_flag = False
elif now>now_1925 and now<now_0325:
    last_run_flag = False
else:
    last_run_flag = True

def check_dummy_execution(execution_list):
    for execution in execution_list:
        start_date = execution['startDate']
        starttime_ind = start_date.astimezone(tz)
        today_date = dt.today().astimezone(tz).replace(hour=4, minute=0)
        if last_run_flag==True:
            difference = today_date-starttime_ind
        else:
            difference = starttime_ind-today_date
        if difference.days==0:
            if execution['status']=='SUCCEEDED':
                stop_date = execution['stopDate']
                exec_time = stop_date-start_date
                if exec_time.seconds<60:
                    execution_list.remove(execution)
    return execution_list

def get_latest_run(SFN_Name):
    global flag_Mapp_flat,flag_Mapp_glprod
    response = sfnClient.list_executions(
        stateMachineArn=SFN_Name
    )
    executions = response['executions']
    if SFN_Name not in exec_time_ignore_list:
        executions = check_dummy_execution(executions)
    latest_execution = executions[0]
    starttimefordiff = latest_execution['startDate']
    starttime_ind = starttimefordiff.astimezone(tz)
    today_date = dt.today().astimezone(tz).replace(hour=4, minute=0)
    if last_run_flag==True:
        difference = today_date-starttime_ind
    else:
        difference = starttime_ind-today_date
    if difference.days!=0:
        latest_execution['status'] = 'WAITING'
    if latest_execution['status']=='WAITING':
        startDates.append('-')
    else:
        startDates.append(starttime_ind.strftime("%b %d, %Y, %H:%M"))
    if 'stopDate' in latest_execution.keys() and latest_execution['status']!='WAITING':
        stopdate_ind = latest_execution['stopDate'].astimezone(tz)
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
    if SFN_Name=='arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_UNSOURCED_SNAPSHOT_WRAPPER' and latest_execution['status']=='WAITING':
        latest_execution['status'] = 'BYPASSED'
    status.append(latest_execution['status'])

for index in range(size):
    SFN_Name = df['SFN'][index]
    get_latest_run('arn:aws:states:us-east-1:245792935030:stateMachine:'+SFN_Name)

df['Status'] = status
df['Start Time'] = startDates
df['End Time'] = endDates
df['Execution Time'] = execTime

print('Sequence No.\tType\tState Machine Name\tStatus\tStart Time\tEnd Time\tExecution Time\n')
countint = 0

df.to_csv('Scripts/SFN_Status_Archive/ODP_MAPP_Daily_Load_Status '+dt_string+'.csv', index=False, encoding='utf-8')

for index in range(size):
    SFN_Name = df['SFN'][index]
    Type = df['Type'][index]
    status1 = df['Status'][index]
    starttime = df['Start Time'][index]
    endtime = df['End Time'][index]
    exectim = df['Execution Time'][index]
    countint+=1
    print(str(countint)+'.\t'+Type+'\t'+SFN_Name+'\t'+status1+'\t'+starttime+'\t'+endtime+'\t'+exectim+'\n')
