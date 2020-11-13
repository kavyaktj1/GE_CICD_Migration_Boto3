import csv
import pandas
import sys
import json

with open('Scripts/config1.json','r') as f:
    data = json.load(f)
    config = data['CSVCreation']

filename = config['filename']
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
flag = "True"

with open(filename) as f:
    lines = f.read().splitlines()
    
def find_files(commit_id,merge_id,mergeid):
    for index in range(len(lines)):
        line = lines[index]
        if line.startswith('commit'):
            line_split = line.split(' ')
            if line_split[1].startswith(merge_id):
                n_line = lines[index+1]
                if n_line.startswith('Merge'):
                    commit_id = merge_id
                    merge_split = n_line.split(' ')
                    merge_id = merge_split[2]
                    find_files(commit_id,merge_id,mergeid)
                else:
                    for ind in range(index+1,len(lines)):
                        lin = lines[ind]
                        if lin.startswith('commit'):
                            return
                        elif lin.startswith('M') or lin.startswith('A') or lin.startswith('R084'):
                            if lin.startswith('R084'):
                                rename_split = lin.split('\t')
                                lin = rename_split[-1].strip()
                            else:
                                lin = lin[1:].strip()
                            if lin.startswith(config['aws_serverless_config_prod']) or lin.startswith(config['aws_serverless_config_qa']):
                                lin_split = lin.split('.')
                                file_path = lin_split[0]+'-'+str(mergeid)
                                file_list.append(file_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['glue_script_id'])!=-1:
                                lin_split = lin.split('.')
                                code_path = lin_split[0]+'-'+str(mergeid)
                                code_list.append(code_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['lambda_script_id'])!=-1:
                                lin_split = lin.split('.')
                                code_path = lin_split[0]+'-'+str(mergeid)
                                lambda_list.append(code_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['dynamodb_config_id'])!=-1:
                                lin_split = lin.split('.')
                                dynamo_path = lin_split[0]+'-'+str(mergeid)
                                dynamo_list.append(dynamo_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['sp_path_id'])!=-1 and lin.endswith(config['sp_ext']):
                                lin = lin[:-3]
                                sp_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['onetimescr_path_id'])!=-1 and lin.endswith(config['onetimescr_ext']):
                                lin = lin[:-4]
                                dbscript_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['view_path_id'])!=-1 and lin.endswith(config['view_ext']):
                                lin = lin[:-5]
                                view_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['ddl_path_id'])!=-1 and lin.endswith(config['ddl_ext']):
                                lin = lin[:-4]
                                ddl_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['sp_path_id'])!=-1 and not lin.endswith(config['sp_ext']):
                                flag = "False"
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['onetimescr_path_id'])!=-1 and not lin.endswith(config['onetimescr_ext']):
                                flag = "False"
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['view_path_id'])!=-1 and not lin.endswith(config['view_ext']):
                                flag = "False"
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['ddl_path_id'])!=-1 and not lin.endswith(config['ddl_ext']):
                                flag = "False"
                   
            
mergeids = sys.argv[1].split(',')
for mergeid in mergeids:
    if len(mergeid)!=0:
        for index in range(len(lines)):
            line = lines[index]
            if line.startswith('commit'):
                line_split = line.split(' ')
                commit_id = line_split[1][0:7]
                n_line = lines[index+1]
                if n_line.startswith('Merge'):
                    merge_split = n_line.split(' ')
                    merge_id = merge_split[2]
                    for ind in range(index+2,len(lines)):
                        lin = lines[ind].strip()
                        if lin.startswith('Merge pull'):
                            l_split = lin.split(' ')
                            if l_split[3]=='#'+str(mergeid):
                                find_files(commit_id,merge_id,mergeid)
                            else:
                                break
 

mergeids = sys.argv[1].split(',')
for mergeid in mergeids:
    if len(mergeid)!=0:
        mergeid_str = '#'+str(mergeid)
        for index in range(len(lines)):
            line = lines[index].strip()
            if mergeid_str in line:
                if line.startswith('*'):
                    continue
                else:
                    for ind in range(index+1,len(lines)):
                        lin = lines[ind].strip()
                        if lin.startswith('commit'):
                            break
                        elif lin.startswith('M') or lin.startswith('A') or lin.startswith('R084'):
                            if lin.startswith('R084'):
                                rename_split = lin.split('\t')
                                lin = rename_split[-1].strip()
                            else:
                                lin = lin[1:].strip()
                            if lin.startswith(config['aws_serverless_config_prod']) or lin.startswith(config['aws_serverless_config_qa']):
                                lin_split = lin.split('.')
                                file_path = lin_split[0]+'-'+str(mergeid)
                                file_list.append(file_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['glue_script_id'])!=-1:
                                lin_split = lin.split('.')
                                code_path = lin_split[0]+'-'+str(mergeid)
                                code_list.append(code_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['lambda_script_id'])!=-1:
                                lin_split = lin.split('.')
                                code_path = lin_split[0]+'-'+str(mergeid)
                                lambda_list.append(code_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['dynamodb_config_id'])!=-1:
                                lin_split = lin.split('.')
                                dynamo_path = lin_split[0]+'-'+str(mergeid)
                                dynamo_list.append(dynamo_path)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['sp_path_id'])!=-1 and lin.endswith(config['sp_ext']):
                                lin = lin[:-3]
                                sp_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['onetimescr_path_id'])!=-1 and lin.endswith(config['onetimescr_ext']):
                                lin = lin[:-4]
                                dbscript_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['view_path_id'])!=-1 and lin.endswith(config['view_ext']):
                                lin = lin[:-5]
                                view_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['ddl_path_id'])!=-1 and lin.endswith(config['ddl_ext']):
                                lin = lin[:-4]
                                lin = str(mergeid)+'@#'+lin
                                ddl_list.append(lin)
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['sp_path_id'])!=-1 and not lin.endswith(config['sp_ext']):
                                flag = "False"
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['onetimescr_path_id'])!=-1 and not lin.endswith(config['onetimescr_ext']):
                                flag = "False"
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['view_path_id'])!=-1 and not lin.endswith(config['view_ext']):
                                flag = "False"
                            elif lin.startswith(config['all_files_search_prefix']) and lin.find(config['ddl_path_id'])!=-1 and not lin.endswith(config['ddl_ext']):
                                flag = "False"
 
file_lists = list(dict.fromkeys(file_list))
for file_var in file_lists:
    file_list = file_var.split('/')
    if len(file_list)>4:
        p_list = []
        service = file_list[3]
        env = file_list[2]
        job_list = file_list[-1]
        pr_id = int(job_list.split('-')[-1])
        pr = '-'+str(pr_id)
        job_name = job_list.replace(pr,'')
        if service=='GlueETL' and len(file_list)==6:
            job_name = file_list[4]+'@#'+job_name
        elif service=='GlueETL' and len(file_list)==5:
            job_name = '@#'+job_name
        p_list.append(service)
        p_list.append(env)
        p_list.append(job_name)
        p_list.append(str(pr_id))
        path_list.append(p_list)

dynamo_lists = list(dict.fromkeys(dynamo_list))
for dyn_var in dynamo_lists:
    dyn_li = dyn_var.split('/')
    p_list = []
    service = 'DynamoDB'
    env = dyn_li[1]
    name_list = dyn_li[-1]
    pr_id = int(name_list.split('-')[-1])
    pr = '-'+str(pr_id)
    table_name = name_list.replace(pr,'')
    p_list.append(service)
    p_list.append(env)
    p_list.append(table_name)
    p_list.append(str(pr_id))
    path_list.append(p_list)
        
code_list = list(dict.fromkeys(code_list))
for code in code_list:
    p_list=[]
    code_li = code.split('/')
    service = 'GlueCode'
    env = code_li[1]
    job_list = code_li[-1]
    pr_id = int(job_list.split('-')[-1])
    pr = '-'+str(pr_id)
    job_name = job_list.replace(pr,'')
    p_list.append(service)
    p_list.append(env)
    p_list.append(job_name)
    p_list.append(str(pr_id))
    path_list.append(p_list)

lambda_list = list(dict.fromkeys(lambda_list))
for lam in lambda_list:
    p_list=[]
    code_li = lam.split('/')
    service = 'LambdaCode'
    env = 'PROD'
    job_list = code_li[-1]
    pr_id = int(job_list.split('-')[-1])
    pr = '-'+str(pr_id)
    job_name = job_list.replace(pr,'')
    p_list.append(service)
    p_list.append(env)
    p_list.append(job_name)
    p_list.append(str(pr_id))
    path_list.append(p_list)
    
sp_list = list(dict.fromkeys(sp_list))
dbscript_list = list(dict.fromkeys(dbscript_list))
view_list = list(dict.fromkeys(view_list))
ddl_list = list(dict.fromkeys(ddl_list))

for sp in sp_list:
    p_list = []
    track = sp.split('/')[1]
    name = sp.split('/')[-1]
    typ = 'sp'
    p_list.append(typ)
    p_list.append(track)
    p_list.append(name)
    p_list.append('-1')
    path_list.append(p_list)
    
for scr in dbscript_list:
    p_list = []
    track = scr.split('/')[1]
    name = scr.split('/')[-1]
    typ = 'dbscript'
    p_list.append(typ)
    p_list.append(track)
    p_list.append(name)
    p_list.append('-1')
    path_list.append(p_list)
    
for view in view_list:
    p_list = []
    track = view.split('/')[1]
    name = view.split('/')[-1]
    typ = 'view'
    p_list.append(typ)
    p_list.append(track)
    p_list.append(name)
    p_list.append('-1')
    path_list.append(p_list)

for ddl in ddl_list:
    p_list = []
    track = ddl.split('/')[1]
    name = ddl.split('/')[-1]
    typ = 'ddl'
    pr_id = ddl.split('@#')[0]
    p_list.append(typ)
    p_list.append(track)
    p_list.append(name)
    p_list.append(pr_id)
    path_list.append(p_list)
            
with open(config['master_csv_file'], "w", newline='') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(csv_header)
    for path in path_list:
        writer.writerow(path)
