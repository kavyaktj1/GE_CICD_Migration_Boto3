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

tz = pytz.timezone('Asia/Kolkata')
now = dt.now().astimezone(tz)
next_day = now+datetime.timedelta(days=1)
now_1125 = now.replace(hour=11, minute=25)
now_1925 = now.replace(hour=13, minute=20)
now_0325 = next_day.replace(hour=0, minute=5)

sfnClient =  boto3.client('stepfunctions',region_name='us-east-1')

last_run_flag = False
DF_list = list()
num_of_frames = 0

master_details = {}

exec_time_ignore_list = ['arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_MASTER_ORDER_SCHEDULE','arn:aws:states:us-east-1:245792935030:stateMachine:MAPP_SPOTFIRE_REFRESH_ORDER']

def get_same_day_exec(execution_list):
    final_list = []
    for execution in execution_list:
        start_date = execution['startDate']
        starttime_ind = start_date.astimezone(tz)
        today_date = dt.today().astimezone(tz).replace(hour=4, minute=0)
        if last_run_flag==True:
            difference = today_date-starttime_ind
        else:
            difference = starttime_ind-today_date
        if difference.days==0:
            final_list.append(execution)
    final_list.reverse()
    return final_list
    
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

def get_executions(latest_execution):
    func_startdate = ''
    func_enddate = ''
    func_exectime = ''
    func_status = ''
    exec_list = []
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
        func_startdate = '-'
    else:
        func_startdate = starttime_ind.strftime("%b %d, %Y, %H:%M")
    if 'stopDate' in latest_execution.keys() and latest_execution['status']!='WAITING':
        stopdate_ind = latest_execution['stopDate'].astimezone(tz)
        func_enddate = stopdate_ind.strftime("%b %d, %Y, %H:%M")
        endtimefordiff = latest_execution['stopDate']
        timediff = endtimefordiff - starttimefordiff
        minutes = divmod(timediff.seconds, 60)
        if minutes[0]>=60:
            hours = int(minutes[0]/60)
            mins = minutes[0]%60
            func_exectime = str(hours)+' hrs, '+str(mins)+' mins, '+str(minutes[1])+' secs'
        else:
            func_exectime = str(minutes[0])+' mins, '+str(minutes[1])+' secs'
    else:
        func_enddate = '-'
        func_exectime = '-'
    func_status = latest_execution['status']
    exec_list.append(func_status)
    exec_list.append(func_startdate)
    exec_list.append(func_enddate)
    exec_list.append(func_exectime)
    return exec_list

def get_sameday_run_status(SFN_Name):
    SFN_arn = 'arn:aws:states:us-east-1:245792935030:stateMachine:'+SFN_Name
    response = sfnClient.list_executions(
        stateMachineArn=SFN_arn
    )
    executions = response['executions']
    if SFN_arn not in exec_time_ignore_list:
        executions = check_dummy_execution(executions)
    final_executions = get_same_day_exec(executions)
    sfn_list = []
    for execution in final_executions:
        sfn_list.append(get_executions(execution))
    master_details[SFN_Name] = sfn_list
    
def get_time_diff(start_time_str,end_time_str):
    if start_time_str=='-' or end_time_str=='-':
        return 90
    start_time = dt.strptime(start_time_str, "%b %d, %Y, %H:%M")
    end_time = dt.strptime(end_time_str, "%b %d, %Y, %H:%M")
    if start_time>end_time:
        return 90
    duration = end_time-start_time
    duration_in_s = duration.total_seconds()
    mins = divmod(duration_in_s, 60)[0]
    return mins

if now>now_1125 and now<now_1925:
    num_of_frames = 0
elif now>now_1925 and now<now_0325:
    num_of_frames = 2
else:
    num_of_frames = 4
    last_run_flag = True
    
filename = 'Scripts/SFN_Details_MAPP_Order.csv'

for index in range(num_of_frames):
    df = pandas.read_csv(filename)
    df = df.dropna(axis = 0, how = 'any')
    DF_list.append(df)
    
main_DF = pandas.read_csv(filename)
main_DF = main_DF.dropna(axis = 0, how = 'any')

size = len(main_DF)

for index in range(size):
    SFN_Name = main_DF['State Machine Name'][index]
    get_sameday_run_status(SFN_Name)
        
for index in range(num_of_frames):
    for sfn_ind in range(size):
        SFN_Name = main_DF['State Machine Name'][sfn_ind]
        run_list = master_details[SFN_Name]
        try:
            if sfn_ind!=0:
                before_ind = sfn_ind-1
                end_time_before = master_details[main_DF['State Machine Name'][before_ind]][index][2]
                specific_run = run_list[index]
                start_time_current = specific_run[1]
                if get_time_diff(end_time_before,start_time_current)>60:
                    DF_list[index]['Status'][sfn_ind] = 'ABORTED'
                    DF_list[index]['Start Time'][sfn_ind] = '-'
                    DF_list[index]['End Time'][sfn_ind] = '-'
                    DF_list[index]['Execution Time'][sfn_ind] = '-'
                    new_list = ['ABORTED','-','-','-']
                    run_list.insert(index,new_list)
                    master_details[SFN_Name] = run_list
                else:
                    run_status = specific_run[0]
                    run_start = specific_run[1]
                    run_end = specific_run[2]
                    run_exec = specific_run[3]
                    DF_list[index]['Status'][sfn_ind] = run_status
                    DF_list[index]['Start Time'][sfn_ind] = run_start
                    DF_list[index]['End Time'][sfn_ind] = run_end
                    DF_list[index]['Execution Time'][sfn_ind] = run_exec
            else:
                specific_run = run_list[index]
                run_status = specific_run[0]
                run_start = specific_run[1]
                run_end = specific_run[2]
                run_exec = specific_run[3]
                DF_list[index]['Status'][sfn_ind] = run_status
                DF_list[index]['Start Time'][sfn_ind] = run_start
                DF_list[index]['End Time'][sfn_ind] = run_end
                DF_list[index]['Execution Time'][sfn_ind] = run_exec
        except Exception as e:
            DF_list[index]['Status'][sfn_ind] = 'WAITING'
            DF_list[index]['Start Time'][sfn_ind] = '-'
            DF_list[index]['End Time'][sfn_ind] = '-'
            DF_list[index]['Execution Time'][sfn_ind] = '-'

if num_of_frames==0:
    new_df = main_DF
    for sfn_ind in range(size):
        new_df['Status'][sfn_ind] = 'NOT STARTED'
    DF_list.append(new_df)
                
for DF in DF_list:
    html_string = DF.to_html(index=False)
    html_string_lines = html_string.split('\n')
    for index in range(len(html_string_lines)):
        line = html_string_lines[index]
        if '<table>' in line:
            html_string_lines[index] = line.replace('<table>','<table style="border: 1px solid black;width:200px">')
        if '</table>' in line:
            html_string_lines[index] = line.replace('</table>','</table><br /><br />')
        if 'SUCCEEDED' in line:
            html_string_lines[index] = line.replace('<td>','<td style="border: 1px solid black;padding: 10px;background-color:#00FF00;white-space:nowrap">')
        elif 'FAILED' in line:
            html_string_lines[index] = line.replace('<td>','<td style="border: 1px solid black;padding: 10px;background-color:#FF0000;white-space:nowrap">')
        elif 'RUNNING' in line:
            html_string_lines[index] = line.replace('<td>','<td style="border: 1px solid black;padding: 10px;background-color:#00FFFF;white-space:nowrap">')
        elif '<td>ORDER</td>' in line:
            html_string_lines[index] = line.replace('<td>','<td style="border: 1px solid black;padding: 10px;background-color:#A033FF;white-space:nowrap">')
        elif '<th>' in line:
            html_string_lines[index] = line.replace('<th>','<th style="border: 1px solid black;padding: 10px;background-color:#FFFF66">')
        elif '<td>' in line:
            html_string_lines[index] = line.replace('<td>','<td style="border: 1px solid black;padding: 10px;white-space:nowrap">')
    final_string = '\n'.join(html_string_lines)
    print(final_string)
