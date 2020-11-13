import json
import boto3
import math
import os
import csv
import collections, functools, operator
from json import dumps
from datetime import date, datetime
from collections import defaultdict
import itertools
import ast
from decimal import Decimal


DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)


############## COMMON VARIABLE #############

#SOURCE_NAME = os.environ["SOURCE_NAME"]

file_size = 512  # Each file max size
min_partition_number = 64  # minimum no of file slices

############################################

class customError(Exception):
    pass

class InputError(Exception):
    pass

class InsertError(Exception):
    pass

###########_FUNCTION_TO_PUT_UPDATED_DATA_TO_DYNAMODB_##############
def ddb_put_item(ddb_list):
    
    print("Dynamo db list : {}".format(ddb_list))
    now = datetime.now()
    upd_date = now.strftime("%d/%m/%Y %H:%M:%S")
    try:
        format_key = {
                "partKey": ddb_list[0][1] + "_mirror_file_split", #source_name
                "sortKey": ddb_list[0][2]#table_name
                }
        
        try:
            ddb_get_response = ddb_table.get_item(Key = format_key)
            job_name = ddb_get_response['Item']['JOB_NAME']
                
        except Exception as err:
            print("error {}".format(err))
                
        format_key1={
                "partKey": ddb_list[0][1] + "_mirror_file_split", #source_name
                "sortKey": ddb_list[0][2],#table_name
                "SIZE_IN_MB": round(Decimal(ddb_list[1]),2),#size in MB
                "FILE_SPLIT": ddb_list[2],#File split
                "PATH": "mirror"+"/"+ ddb_list[0][1] + "/" + ddb_list[0][2]
                }
            
        format_key1.update({"JOB_NAME": job_name, "UPDATED_DATE": upd_date})
        ddb_put_response = ddb_table.put_item(Item=format_key1)
        
        print("Insert {}".format(ddb_put_response))
        return 'Success'
        
    except Exception as e:
        print("Put error {}".format(e))
       # raise InsertError(e)

def aws_client(srv):
    try:
        clnt = boto3.client(srv, region_name=os.environ["region"])
        return clnt

    except Exception as e:
        print("Client error {}".format(e))

############_GET_CURRENT_BATCH_FROM_DYNAMODB_################
def dynamodb_get_current_batch(lambda_nam,src_nam):
    client = aws_client('lambda')
    src_name = src_nam.upper()
    format_key = {
                "INPUT":
                        {
                            "SRC_NAME":src_name
                        }
                }
    response = client.invoke(FunctionName=lambda_nam,#"ddb_get_lambda",
                                           Payload=json.dumps(format_key))
    responseFromChild = json.load(response['Payload'])
    return responseFromChild
###############################################################



###########_TO_GET_THE_SIZE_OF_EACH_OBJECT_INSIDE_TABLE_############
def get_src_level_batch_size_list(mrr_bucket_name,mrr_file_prefix,lambda_nam,src_nam):
    print("Inside core block ")
    temp_list = []
    temp_list_1 = []

    s3client = aws_client('s3')
    delimit = "/"
    print("bucket name {} ".format(mrr_bucket_name))
    print("file prefix name {} ".format(mrr_file_prefix))
    theobjects = s3client.list_objects_v2(Bucket=mrr_bucket_name, Prefix=mrr_file_prefix,
                                                  Delimiter=delimit)
    paginator = s3client.get_paginator('list_objects_v2')
    s3_response = paginator.paginate(Bucket=mrr_bucket_name, Prefix=mrr_file_prefix)
    #print("se_response {}".format(s3_response))
    table_list = []

    for x in theobjects['CommonPrefixes']:
        table_list1 = x['Prefix'] + "batch=" + dynamodb_get_current_batch(lambda_nam,src_nam) #'1'
        table_list.append(table_list1)
        
    print("table_list {}".format(table_list))
    for response in s3_response:
        for i in response['Contents']:
            temp_dict = {}
            temp_dict_1 = {}
            get_batch_no = i['Key'] #mirror/gla/gl_jornl_det_all_f/
            dirnam=os.path.dirname(get_batch_no)
            
            for table in table_list:
                if table == dirnam :
                    temp_dict.update({i['Key']: i['Size']})
                    temp_list.append(temp_dict)
                    temp_dict_1.update({table: i['Size']})
                    temp_list_1.append(temp_dict_1)
    print('After Key filter {}, length {}'.format(temp_list_1, len(temp_list_1)))
    return temp_list_1
#####################################################################################


#############_TO_CALCULATE_THE_NUMBER_OF_FILE_SPLIT_#################################
def s3_list_get_objects(mrr_bucket_name, SOURCE_NAME, FUNCTION_NAME):
    s3client = aws_client('s3')
    delimit = "/"
    final_result ={}
    try:
        print("Source name input is a list {}".format(SOURCE_NAME))
        # Converting string to list 
        SOURCE_NAME_LIST = SOURCE_NAME.split(',')#ast.literal_eval(SOURCE_NAME)
        print("SOURCE_NAME_LIST {} {}".format(SOURCE_NAME_LIST,type(SOURCE_NAME_LIST)))
        for src in SOURCE_NAME_LIST:
            if src.upper() == 'ALL':
                print("Source name input is ALL")
                Sourceobjects = s3client.list_objects_v2(Bucket=mrr_bucket_name, Prefix='mirror/',
                                                    Delimiter=delimit)
                source_list = []
                for source in Sourceobjects['CommonPrefixes']:
                    print("Src cmnprefix {}:".format(source))
                    source_list1 = source['Prefix'] 
                    source_list.append(source_list1)
                print("source_list {}".format(source_list))                                     
                for src_prefix in source_list:
                    final_batch_list1 = get_src_level_batch_size_list(mrr_bucket_name,src_prefix,FUNCTION_NAME,src)
                    #final_batch_list.append(final_batch_list1)
                    if len(final_batch_list1) ==0:
                        pass
                    else:
                        result = dict(functools.reduce(operator.add,
                                       map(collections.Counter, final_batch_list1)))
                    final_result.update(result)
                
            else:
                print("Source name input is given one source")
                src_prefix = 'mirror/' + src.lower() + '/'
                final_batch_list1 = get_src_level_batch_size_list(mrr_bucket_name,src_prefix,FUNCTION_NAME,src)
                if len(final_batch_list1) ==0:
                        pass
                else:
                    result = dict(functools.reduce(operator.add, map(collections.Counter, final_batch_list1)))
                    
                final_result.update(result)
                
        print("final_batch_list1 {}".format(final_batch_list1))        
        
        result = final_result
        #print("result {}".format(result))
        result1 = {**result}
        result1.update((x,round((y * 0.00000095367432),2)) for x,y in result1.items()) #to get the size in MB
        result2 = {**result}
        result2.update((x,[x,x.split('/')[1],x.split('/')[2]]) for x in result2.keys()) #spliting prefix, sourcename, tablename
        print("result1 {}".format(result1))
        print("result2 {}".format(result2))

        
        for x,y in result.items():
            size = (math.ceil(y * 0.00000095367432)) #size in MB
            #print("size {}".format(size))
            if (int(size)) < 64:
                print("file {} is of size {} MB therefore {} will be the file split".format(x,size,size))
                result.update({x:size})
            else:
                result.update(
                    {x:(math.ceil(((y * 0.00000095367432) / file_size) / min_partition_number) * min_partition_number)})
        print("resultant dictionary : ", str(result))
        
        dd = defaultdict(list)
        
        for d in (result2, result1, result): 
            for key, value in d.items():
                dd[key].append(value)
        
        #print(dd)
        ddb_otp_lst = dd.values()
        print(ddb_otp_lst)
        #Writing Data to Dynamo DB
        ddb_list=[]
        for i in ddb_otp_lst:
            ddb_dict={}
            ddb_dict.update({"partKey":i[0][1] + "_mirror_file_split"})
            ddb_dict.update({"sortKey":i[0][2]})
            ddb_dict.update({"SIZE_IN_MB":round(Decimal(i[1]),2)})
            ddb_dict.update({"FILE_SPLIT":i[2]})
            ddb_dict.update({"PATH":"mirror/"+i[0][1] + "/"+i[0][2]})
            ddb_list.append(ddb_dict)
        print("ddb_input_formatted: {}".format(ddb_list))
        #ddb_put_item(ddb_otp_lst)
        print("result {}".format(result))

        return result,ddb_otp_lst

    except Exception as e:
        print("Please check the input ,Exception in core block {}".format(e))
        raise customError("Expection at core block {}".format(e))
        
#########################################################################################

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))
    


def glue_describe_job(glue_job_name):
    try:
        glue_client = aws_client('glue')
        job_desc_response = glue_client.get_job(JobName=glue_job_name)
        #print("job_desc_response[job] {}".format(job_desc_response["Job"]))
        job_details = json.dumps(job_desc_response["Job"], default=json_serial)
        return job_details

    except Exception as e:
        print("Unable to describe job {}".format(e))
        raise customError("Unable to describe job")



def validate_glue_job_availability(part_key, sort_key):

    try:

        format_key = {
            "partKey": part_key,
            "sortKey": sort_key
        }
        
        print("partKey , sortKey {}, {}".format(part_key,sort_key))
        ddb_get_response = ddb_table.get_item(Key = format_key)
        print("Get response {}".format(ddb_get_response))

        return (ddb_get_response['Item']['JOB_NAME'])

    except Exception as e:

        print("no job present in glue for this combination = {}_{}".format(part_key,sort_key))
        return False


def glue_update_job_argument(calculate_metrics,ddb_upd):

    try:

        job_available_list = []
        job_missing_list = []
        job_total = 0

        for job_partition_key, job_split_value in calculate_metrics.items():
            job_total += 1
            
            part_key = job_partition_key.split('/')[1] + "_mirror_file_split"
            sort_key = job_partition_key.split('/')[2]
            print("cal_matrix_part_key {}".format(part_key))
            print("cal_matrix_sort_key {}".format(sort_key))
            src_name_table_name = job_partition_key.split('/')[1] + "_" + job_partition_key.split('/')[2]

            glue_job_avail_status = validate_glue_job_availability(part_key,
                                                                   sort_key)
            print("glue_job_avail_status {}".format(glue_job_avail_status))
            if glue_job_avail_status:
            
                job_available_list.append(src_name_table_name)

                glue_client = aws_client('glue')

                #glue_job_description = glue_describe_job(glue_job_avail_status)
                #job_lnd_mrr_incr_refresh_ov_chu_bix_complaint
                glue_job_description = glue_describe_job(glue_job_avail_status)
                print("glue_job_description {}".format(glue_job_description))

                mod_config = json.loads(glue_job_description)
                print("mod_config {}".format(mod_config))

                temp_def_args = {}
                mod_config_dif_arg=mod_config['DefaultArguments']
                print("mod_config_dif_arg {}".format(mod_config_dif_arg))

                for k, v in mod_config_dif_arg.items():

                    temp_def_args.update({k: v})
                    #print("temp_def_args {}".format(temp_def_args))
                    if k == "--file_split":
                        print("job_split_value {}".format(job_split_value))
                        temp_def_args.update({k: str(job_split_value)})

                mod_config['DefaultArguments'] = temp_def_args
                # ddb_job_nam = mod_config["Name"]
                # print("ddb_job_nam {}".format(ddb_job_nam))

                update_payload = {}
                update_payload["JobName"] = mod_config["Name"]



                del mod_config["Name"]
                del mod_config["CreatedOn"]
                del mod_config["LastModifiedOn"]
                del mod_config["MaxCapacity"]
                # del mod_config["AllocatedCapacity"]

                update_payload["JobUpdate"] = mod_config
                  
                try:
                    if len(update_payload["JobUpdate"]["WorkerType"]) != 0 and update_payload["JobUpdate"]["NumberOfWorkers"] > 0:
                        del update_payload["JobUpdate"]["AllocatedCapacity"]
                
                except KeyError as ke:
                    print("error {}".format(ke))
                    if ke.args[0] == 'WorkerType':
                        del update_payload["JobUpdate"]["AllocatedCapacity"]
                
                print("updated payload {}".format(update_payload))
                update_glue_job_response = glue_client.update_job(**update_payload)

                print("Glue update response {}".format(update_glue_job_response))
                for ddb_list in ddb_upd:
                    src_nam_batch_num = ddb_list[0][0]
                    if src_nam_batch_num == job_partition_key:
                        print("job_part_key ,ddb_list {} #####{}".format(job_partition_key,ddb_list))
                        ddb_put_item(ddb_list)

                print("Glue updated ")

            else:
                job_missing_list.append(src_name_table_name)

        print("Available {}, Missing {}, Total {}".format(len(job_available_list), len(job_missing_list), job_total))
    
    except KeyError as ke:
        print("Glue job Key error {}".format(ke))
        pass
    
    except Exception as e:
        print("Glue job update error {}".format(e))
        pass
        # raise customError("Glue update error {}".format(e))

def lambda_handler(event, context):

    try:
        MRR_BUCKET_NAME = os.environ["BUCKET_NAME"]
        MRR_FILE_PREFIX = os.environ["MRR_FILE_PREFIX"]
        MANIFEST_BUCKET_NAME = os.environ["MANIFEST_BUCKET_NAME"]
        MANIFEST_FILE_PREFIX = os.environ["MANIFEST_FILE_PREFIX"]
        SOURCE_NAME = event['SRC_NAME']
        FUNCTION_NAME = os.environ['CHILD_FUNC_NAME']

        CURRENT_ACCOUNT_NUMBER = context.invoked_function_arn.split(":")[4]
        LAMBDA_NAME = "arn:aws:lambda:" + os.environ["region"] + str(CURRENT_ACCOUNT_NUMBER) +":function:" + "ddb_get_lambda"
        s3_list_get_objects_response ,ddb_upd = s3_list_get_objects(MRR_BUCKET_NAME,SOURCE_NAME,FUNCTION_NAME)
        glue_update = 0
        if s3_list_get_objects_response:
            
            glue_update_job_argument(s3_list_get_objects_response,ddb_upd)
            print("glue update call : ".format(glue_update))

    except Exception as e:
        print("Error in lambda: {}".format(e))
        raise customError("Lambda Failed")

