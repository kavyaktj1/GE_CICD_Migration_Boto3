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
dir_name = 'tempdir_odp'
track_name = sys.argv[2].strip()
if track_name!='All':
    track_name = '/'+track_name
else:
    track_name = ''
migration_component = sys.argv[3]

def get_extension(migration_component):
    lower_migration_comp = migration_component.lower().strip()
    if lower_migration_comp=='tables':
        return 'ddl'
    elif lower_migration_comp=='stored procedures':
        return 'sp'
    elif lower_migration_comp=='views':
        return 'view'
    elif lower_migration_comp=='one time scripts':
        return 'scr'

for root, dirs, files in os.walk(path_prefix+'/'+dir_name+'/ODP'+track_name):
    for file in files:
        comp_path = os.path.join(root, file)
        comp_path = comp_path.replace(path_prefix+'/'+dir_name+'/','')
        comp_ext = get_extension(migration_component)
        if file.endswith("."+comp_ext) and track_name in comp_path:
            if file.endswith(".ddl"):
                comp_path = comp_path.replace('.'+comp_ext,'')
                comp_path = '-1@#'+comp_path
                ddl_list.append(comp_path)
            elif file.endswith(".sp"):
                comp_path = comp_path.replace('.'+comp_ext,'')
                sp_list.append(comp_path)
            elif file.endswith(".view"):
                comp_path = comp_path.replace('.'+comp_ext,'')
                view_list.append(comp_path)
            elif file.endswith(".scr"):
                comp_path = comp_path.replace('.'+comp_ext,'')
                dbscript_list.append(comp_path)

    
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
            
with open("job_list.csv", "w", newline='') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerow(csv_header)
    for path in path_list:
        writer.writerow(path)