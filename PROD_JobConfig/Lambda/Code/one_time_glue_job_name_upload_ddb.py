import json
import boto3
import os
import operator
import os
import collections, operator
from json import dumps
from datetime import date, datetime
from collections import defaultdict


DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]
job_list=[]
valid_job =[]
available_job_detail =[]


ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)


class InsertError(Exception):
    pass

class InputError(Exception):
    pass


s3_client = boto3.client('glue',region_name=DDB_REGION)
response = s3_client.list_jobs()
print("response {}".format(response))

def get_glue_job_names():
    """get_glue_job_names to get the lsit of all glue jobs"""
    
    response = s3_client.list_jobs()
    for job_name in response['JobNames']:
        job_list.append(job_name)
    #job_list=[]
    i=0
    while True:
        
        if 'NextToken' in response:
            response = s3_client.list_jobs(NextToken=response['NextToken'])
        else:
            response = s3_client.list_jobs()

        for job_name in response['JobNames']:
            job_list.append(job_name)

        if 'NextToken' not in response:
            break
    return False


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def glue_describe_job(glue_job_name):
    """this function will get the job details from glue"""
    try:
        job_desc_response = s3_client.get_job(JobName=glue_job_name)
        job_details = json.dumps(job_desc_response["Job"], default=json_serial)
        return job_details

    except Exception as e:
        print("Unable to describe job {}".format(e))
        raise customError("Unable to describe job")


def write_job_to_ddb(valid_job):
    """ write job_name, src_name, table_name to DynamoDB"""
    
    unavailable_job_detail_cnt =0
    for job in valid_job:
        glue_job_description = glue_describe_job(job)
        mod_config = json.loads(glue_job_description)
        try:
            PART_KEY = mod_config['DefaultArguments']['--src_system'] + "_mirror_file_split"
            SORT_KEY= mod_config['DefaultArguments']['--mrr_table_name']
            PATH= "mirror"+"/"+ mod_config['DefaultArguments']['--src_system'] + "/" + mod_config['DefaultArguments']['--mrr_table_name']
            JOB_NAME= mod_config['Name']
            
            format_key={
                    "partKey": PART_KEY,
                    "sortKey": SORT_KEY,
                    "PATH": PATH,
                    "JOB_NAME": JOB_NAME
                    }
                    
            ddb_put_response = ddb_table.put_item(Item=format_key)
            available_job_detail.append(format_key)
            
        except Exception as e:
            print("Key --key not available {}".format(e))
            unavailable_job_detail_cnt += 1
            pass

    print("unavilable_job_detail {}".format(unavailable_job_detail_cnt))    
        
        

def lambda_handler(event, context):

    try:
        get_glue_job_names()
        print("job_list {}".format(job_list))
        [valid_job.append(job) for job in job_list if operator.contains(job, "job_lnd_mrr_full_refresh_")
        or operator.contains(job, "job_lnd_mrr_incr_refresh_ov_") or operator.contains(job, "job_lnd_mrr_incr_refresh_no_ov_")
        or operator.contains(job, "job_lnd_mrr_incr_refresh_ov_chu_bix_complaint_")]
        
        print("valid_job {}".format(valid_job))
        write_job_to_ddb(valid_job)
        
        print("job_list = {}, valid_job = {} , available_job_detail = {}".format(len(job_list),len(valid_job),len(available_job_detail)))
        print("available_job_detail {}".format(available_job_detail))
    
        return "sucess"
    
    except Exception as e:
        print("Error in lambda: {}".format(e))
        raise customError("Lambda Failed")
