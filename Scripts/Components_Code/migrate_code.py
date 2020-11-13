import csv
import pandas
import json
import os

with open('../config.json','r') as f:
    data = json.load(f)
    config = data['Code']

df = pandas.read_csv(config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')

size = len(df)

def get_rollback_pr_id(jobname,pr_id,service):
    if service=='Glue':
        path = config['glue_python_file']
    else:
        path = config['lambda_python_file']
    files_pr = []
    pr_id = int(pr_id)
    for i in os.listdir(path):
        if os.path.isfile(os.path.join(path,i)) and jobname in i:
            file_list = i.split('.')
            prid = int(file_list[0].split('-')[-1])
            files_pr.append(prid)
    max_pr = 0
    for pr in files_pr:
        if pr<pr_id and pr>max_pr:
            max_pr = pr
    return max_pr

def migrate_glue_code(track,job,pr_id):
    try:
        rollback_pr = str(get_rollback_pr_id(job,pr_id,'Glue'))
        if rollback_pr=='0':
            with open(config['glue_code_prefix']+track+config['glue_code_path']+job+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
                python_code = f.read()
                with open(config['glue_python_file']+job+'-'+pr_id+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
                    outfile.write(python_code)
        else:
            with open(config['glue_python_file']+job+'-'+rollback_pr+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
                python_code = f.read()
                with open(config['glue_rollback_path']+job+'-'+pr_id+'-'+rollback_pr+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
                    outfile.write(python_code)
            with open(config['glue_code_prefix']+track+config['glue_code_path']+job+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
                python_code = f.read()
                with open(config['glue_python_file']+job+'-'+pr_id+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
                    outfile.write(python_code)
    except FileNotFoundError as e:
        print("File Not Found : ",job)

def migrate_lambda_code(job,pr_id):
    try:
        rollback_pr = str(get_rollback_pr_id(job,pr_id,'Lambda'))
        if rollback_pr=='0':
            with open(config['lambda_code_prefix']+job+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
                python_code = f.read()
                with open(config['lambda_python_file']+job+'-'+pr_id+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
                    outfile.write(python_code)
        else:
            with open(config['lambda_python_file']+job+'-'+rollback_pr+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
                python_code = f.read()
                with open(config['lambda_rollback_path']+job+'-'+pr_id+'-'+rollback_pr+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
                    outfile.write(python_code)
            with open(config['lambda_code_prefix']+job+'.py', 'r') as f: # Storing the initial config in the 'Input' folder.
                python_code = f.read()
                with open(config['lambda_python_file']+job+'-'+pr_id+'.py', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
                    outfile.write(python_code)
    except FileNotFoundError as e:
        print("File Not Found : ",job)

for index in range(size):
    service = df['Service'][index]
    track = df['Track'][index]
    job = df['JobName'][index]
    pr_id = str(df['PR_ID'][index])
    if service=='GlueCode':
        migrate_glue_code(track,job,pr_id)
    elif service=='LambdaCode':
        migrate_lambda_code(job,pr_id)