import csv
import pandas
import sys
import os
from os import listdir
from os.path import isfile, join
import re

path_list = []
csv_header = ['Service','Environment','JobName','PR_ID']
file_list = []
code_list = []
lambda_list = []
sfn_list = []
sp_list = []
dbscript_list = []
view_list = []
ddl_list = []
dynamo_list = []

path_prefix = sys.argv[1]
service_name = sys.argv[2].strip()

for root, dirs, files in os.walk(path_prefix+'/Backup/PROD/'+service_name):
    for file in files:
        comp_path = os.path.join(root, file)
        if comp_path.endswith('.json'):
            job_name = comp_path.split('/')[-1].replace('.json','')
            job_name = job_name.split('\\')[-1]
            file_list.append(job_name)

file_lists = list(dict.fromkeys(file_list))
for file_var in file_lists:
    p_list = []
    service = service_name
    env = 'PROD'
    job_list = file_var
    job_name = file_var
    pr_id = 1
    if service=='GlueETL':
        job_name = 'Backup@#'+file_var
    p_list.append(service)
    p_list.append(env)
    p_list.append(job_name)
    p_list.append(str(pr_id))
    path_list.append(p_list)
            
with open("Scripts/job_list_nprod.csv", "w", newline='') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(csv_header)
    for path in path_list:
        writer.writerow(path)