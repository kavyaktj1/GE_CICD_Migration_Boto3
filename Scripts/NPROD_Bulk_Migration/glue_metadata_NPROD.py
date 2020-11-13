# This script will get the Glue Job config for DEV Environment from Git and modify the configuration according to PROD Environment and store the updated config
# in 'Output' Folder.

import sys
import boto3
import time
import json
import re
import pandas
from time import sleep

with open('../config_NPROD.json','r') as f:
    data = json.load(f)
    config = data['GlueETLConfigNPROD']

glue = boto3.client('glue',region_name=config['region_name'])

df = pandas.read_csv('../GlueETL/'+config['job_name_list'])
df = df.dropna(axis = 0, how = 'any')
path_prefix = sys.argv[1]


def change_default_arguments(defarg): # Modifies the default arguments in the DEV config file according to PROD requirements
    defarg['--TempDir'] = config['--TempDir']
    defarg['--bucket_name'] = config['--bkt_name']
    defarg['--extra-py-files'] = config['--extra-py-files']
    defarg['--job-bookmark-option'] = config['--job-bookmark-option']
    defarg['--envprefix'] = config['--envprefix']
    if '--arn_acc_no' in defarg:
        defarg['--arn_acc_no'] = config['--arn_acc_no']
    return defarg

def change_script_loc(command,track,filename): # Changes the Track and prefix in script location path.
    script_loc = command['ScriptLocation']
    # f_name = script_loc.split('/')[-1]
    # if track=='Entity':
        # newloc = 's3://odp-us-nprod-servicesuite/common/502818085/ODP/Common/src/glue/Entity/'+f_name
    # elif track=='DQ_Framework':
        # newloc = 's3://odp-us-nprod-servicesuite/common/502818085/ODP/Common/src/glue/DQ_Framework/'+filename+'.py'
    # elif track=='Compaction':
        # newloc = config['Script_location_prefix']+f_name
    # else:
        # newloc = 's3://odp-us-nprod-servicesuite/common/502818085/ODP/'+track+'/src/glue/'+f_name
    newloc = script_loc.replace('s3://odp-us-prod-raw/servicesuite','s3://odp-us-nprod-servicesuite/common/502818085')
    command['ScriptLocation'] = newloc
    return command
    
def change_connection(connections): # Changes the glue connections according to PROD.
    conn = connections['Connections']
    conn[0] = config['--glue_conn']
    connections['Connections'] = conn
    return connections
    
def modify_shell_params(defarg):
    if '--compaction_layer_base_path' in defarg:
        defarg['--compaction_layer_base_path'] = defarg['--compaction_layer_base_path'].replace('innovation',config['--compaction_layer_base_path'])
    if '--database_name' in defarg:
        defarg['--database_name'] = config['--database_name']
    if '--host' in defarg:
        defarg['--host'] = config['--host']  
    if '--iam_role' in defarg:
        defarg['--iam_role'] = config['--iam_role']
    if '--input_json_bucket' in defarg:
        defarg['--input_json_bucket'] = config['--input_json_bucket']
    if '--odp_env' in defarg:
        defarg['--odp_env'] = config['--odp_env']
    if '--dynamo_mirror_metadata_table' in defarg:
        defarg['--dynamo_mirror_metadata_table'] = config['--dynamo_mirror_metadata_table']
    if '--redshift_password_key' in defarg:
        defarg['--redshift_password_key'] = config['--redshift_password_key']
    if '--redshift_secret_key' in defarg:
        if 'daasbatchextract' in defarg['--redshift_secret_key']:
            defarg['--redshift_secret_key'] = 'us-nprod-daasbatchextract-fsso'
        else:
            defarg['--redshift_secret_key'] = config['--redshift_secret_key']
    if '--redshift_username_key' in defarg:
        defarg['--redshift_username_key'] = config['--redshift_username_key']
    if '--aws_secret' in defarg:
        defarg['--aws_secret '] = config['shell_redshift_secret']
    if '--bucket_name' in defarg:
        defarg['--bucket_name'] = config['--bkt_name']
    if '--extra-py-files' in defarg:
        defarg['--extra-py-files'] = config['shell-extra-py-files']
    if '--key' in defarg:
        defarg['--key'] = config['shell--key']
    if '--dynamo_input_table' in defarg:
        table_dynamo_input = defarg['--dynamo_input_table'].replace(config['--dynamo_input_table_before'],config['--dynamo_input_table_after'])
        defarg['--dynamo_input_table'] = table_dynamo_input
    if '--blue_print_bucket' in defarg:
        defarg['--blue_print_bucket'] = config['--bkt_name']
    if '--extract_output_bucket' in defarg:
        defarg['--extract_output_bucket'] = config['--extract_output_bucket']
    return defarg
        
        
size = len(df)

for index in range(size):
    job = df['JobName'][index]
    track = df['Track'][index]
    daas_flag = False
    with open(path_prefix+'/Backup/PROD/GlueETL/'+job+'.json','r') as f: # Getting the config file from Git.
        glue_job = json.load(f)
        with open(path_prefix+config['JSON_Git_Input']+job+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the initial config in the 'Input' folder.
            json.dump(glue_job, outfile)
        time.sleep(0.5)
        glue_job['Role'] = config['--aws_iam_role'] # Changing the role of job.
        if daas_flag==False:
            glue_job['ExecutionProperty'] = {
                    "MaxConcurrentRuns": 1
                }
        job_type = glue_job['Command']['Name']
        glue_job['Command'] = change_script_loc(glue_job['Command'],df['Track'][index],glue_job['Name']) # Changing the script location of job.
        glue_job['Name'] = glue_job['Name'] 
        if 'Connections' not in glue_job:
            glue_job['Connections'] = {
                "Connections": [
                    "us-nprod-peering-redshift"
                ]
            }
        glue_job['Connections'] = change_connection(glue_job['Connections'])
        glue_job['CreatedOn'] = str(glue_job['CreatedOn'])
        glue_job['LastModifiedOn'] = str(glue_job['LastModifiedOn'])
        #glue_job['GlueVersion'] = "1.0"
        glue_job['MaxRetries'] = 0
        if job_type=='pythonshell':
            glue_job['DefaultArguments'] = modify_shell_params(glue_job['DefaultArguments'])
        else:
            glue_job['DefaultArguments'] = change_default_arguments(glue_job['DefaultArguments'])
            if 'MaxCapacity' not in glue_job:
                glue_job['MaxCapacity'] = 10
        
        #glue_job['DefaultArguments'] = change_source_db(glue_job['DefaultArguments'])
        #if 'WorkerType' not in glue_job.keys():
            #glue_job['WorkerType'] = 'Standard'
            #glue_job['NumberOfWorkers'] = 4
        #glue_job['Tags'] = {'Track':df['Track'][index]} # Adding Tags in form of Track which is retreived from file list.
        with open(path_prefix+config['JSON_File_PROD']+job+'.json', 'w',encoding = 'UTF-8') as outfile: # Storing the final config in 'Output' folder.
            json.dump(glue_job, outfile, indent=4)
