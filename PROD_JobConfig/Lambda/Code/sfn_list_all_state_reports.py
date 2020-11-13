import json
import datetime
import csv
import os
import boto3
import re

sfn_client = boto3.client("stepfunctions")
S3_CLINET = boto3.client('s3')

def reformat_list(list_val):
    r_list = []
    if list_val != None:
        for h in range(len(list_val)):
            for i in list_val[h]:
                if i != None:
                    for k, v in i.items():
                        #print(v)
                        r_list.append(v)
    else:
        print("PRajj ================= {}".format(list_val))
    return r_list

def generate_csv_report(history_resp_list, s3_bucket_name, s3_folder_path):
    
    s3_file_name = s3_folder_path +'sfn_job_report-'+str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')) + '.csv'
    tmp_file_name = '/tmp/sfn_job_report.csv'
    

    #kys = history_resp_list[0].keys()

    with open(tmp_file_name, 'w', newline='') as output_file:
            
        header = ["STATEMACHINE_NAME","ARN_NO","START_DATE","ARN_JOB_STATUS","JOB_NAME","START_TIME","STOP_TIME","TASK_STATUS","START_STOP_DIFF_SEC","START_STOP_DIFF_MINS"]
        dict_writer = csv.writer(output_file)
        dict_writer.writerow(header)
        n=len(header)
        history_rep = [history_resp_list [i:i + n] for i in range (0, len (history_resp_list), n)]
        '''history_rep_filtered = []
        for ele in history_rep:
            if ele in history_rep:
                if ele[4][0:10]==str(datetime.datetime.now().strftime('%Y-%m-%d')):
                    history_rep_filtered.append(ele)'''
        dict_writer.writerows(history_rep)

    # Upload file to s3 from temp folder.
    S3_CLINET.upload_file(tmp_file_name, s3_bucket_name, s3_file_name)


def conv_int_date_time_format(timestamp_with_ms):
	dt = datetime.datetime.fromtimestamp(timestamp_with_ms / 1000)
	formatted_time = str(dt.strftime('%Y-%m-%d %H:%M:%S')[0:22])
	#print("-----type of {}".format(type(formatted_time)))
	return formatted_time


def sub_date_min(StartedOn, CompletedOn):
   StartDate_dt = datetime.datetime.fromtimestamp(StartedOn / 1000)
   StopDate_dt = datetime.datetime.fromtimestamp(CompletedOn / 1000)
   time_delta = (StopDate_dt - StartDate_dt)
   total_seconds = time_delta.total_seconds()
   total_minutes = total_seconds/60
   return total_minutes

def sub_date_sec(StartedOn, CompletedOn):
   StartDate_dt = datetime.datetime.fromtimestamp(StartedOn / 1000)
   StopDate_dt = datetime.datetime.fromtimestamp(CompletedOn / 1000)
   time_delta = (StopDate_dt - StartDate_dt)
   total_seconds = time_delta.total_seconds()
   return total_seconds

def convert_date_format(in_date):
    dt_fmt = in_date.strftime('%Y-%m-%d %H:%M:%S')
    return dt_fmt


def calc_time_difference_min(dt_one, dt_two):
    start_dt = datetime.datetime.strptime(str(dt_one), '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.datetime.strptime(str(dt_two), '%Y-%m-%d %H:%M:%S')

    time_difference = end_dt - start_dt
    time_difference_mins = (time_difference.total_seconds() / 60)

    return time_difference_mins
    
def calc_time_difference_sec(dt_one, dt_two):
    start_dt = datetime.datetime.strptime(str(dt_one), '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.datetime.strptime(str(dt_two), '%Y-%m-%d %H:%M:%S')

    time_difference = end_dt - start_dt
    time_difference_sec = (time_difference.total_seconds())

    return time_difference_sec



def sfn_get_exe_history(exe_arn):
    try:
        temp_list = []
        for i in exe_arn:
            exe_response=sfn_client.get_execution_history(executionArn=i["executionArn"])
            for x in exe_response["events"]:
                temp_dict = {}
                split_data = i["executionArn"].split(':')
                temp_dict["STATEMACHINE_NAME"] = split_data[6]
                temp_dict["ARN_NO"] = split_data[7]
                temp_dict["START_DATE"] = ''
                temp_dict["ARN_JOB_STATUS"] = i["ARN_status"]
                
                if x["type"] == "TaskSucceeded":
                    parse_x_TaskSucceeded = json.loads(x["taskSucceededEventDetails"]["output"])
                    if "CompletedOn" in parse_x_TaskSucceeded:
                        if "JobName" in parse_x_TaskSucceeded:
                            temp_dict["JOB_NAME"] = parse_x_TaskSucceeded["JobName"]
                        if "StartedOn" in parse_x_TaskSucceeded:
                            temp_dict["START_TIME"] = conv_int_date_time_format(parse_x_TaskSucceeded["StartedOn"])
                            temp_dict["START_DATE"] = temp_dict["START_TIME"][0:10]
                        if "CompletedOn" in parse_x_TaskSucceeded:
                            temp_dict["STOP_TIME"] = conv_int_date_time_format(parse_x_TaskSucceeded["CompletedOn"])
                        if "JobRunState" in parse_x_TaskSucceeded:
                            temp_dict["JOB_STATUS"] = parse_x_TaskSucceeded["JobRunState"]
                        if "StartedOn" in parse_x_TaskSucceeded and "CompletedOn" in parse_x_TaskSucceeded:
                            temp_dict["START_STOP_DIFF_SEC"] = round(sub_date_sec(parse_x_TaskSucceeded["StartedOn"], parse_x_TaskSucceeded["CompletedOn"]))
                        if "StartedOn" in parse_x_TaskSucceeded and "CompletedOn" in parse_x_TaskSucceeded:
                            temp_dict["START_STOP_DIFF_MINS"] = round(sub_date_min(parse_x_TaskSucceeded["StartedOn"], parse_x_TaskSucceeded["CompletedOn"]),2)
                        if temp_dict:
                            temp_list.append(temp_dict)
                
                if x["type"] == "TaskFailed":
                    parse_x_TaskFailed = json.loads(x["taskFailedEventDetails"]["cause"])
                    if "CompletedOn" in parse_x_TaskFailed:
    
                        if "JobName" in parse_x_TaskFailed:
                            temp_dict["JOB_NAME"] = parse_x_TaskFailed["JobName"]
                        if "StartedOn" in parse_x_TaskFailed:
                            temp_dict["START_TIME"] = str(conv_int_date_time_format(parse_x_TaskFailed["StartedOn"]))

                            temp_dict["START_DATE"] = temp_dict["START_TIME"][0:10]
                        if "CompletedOn" in parse_x_TaskFailed:
                            temp_dict["STOP_TIME"] = conv_int_date_time_format(parse_x_TaskFailed["CompletedOn"])
                        if "JobRunState" in parse_x_TaskFailed:
                            temp_dict["JOB_STATUS"] = parse_x_TaskFailed["JobRunState"]
                        if "StartedOn" in parse_x_TaskFailed and "CompletedOn" in parse_x_TaskFailed:
                            temp_dict["START_STOP_DIFF_SEC"] = round(sub_date_sec(parse_x_TaskFailed["StartedOn"], parse_x_TaskFailed["CompletedOn"]))
                        if "StartedOn" in parse_x_TaskFailed and "CompletedOn" in parse_x_TaskFailed:
                            temp_dict["START_STOP_DIFF_MINS"] = round(sub_date_min(parse_x_TaskFailed["StartedOn"], parse_x_TaskFailed["CompletedOn"]),2)
                        if temp_dict:
                            temp_list.append(temp_dict)
                else:
                    if x["type"] == "ExecutionFailed":
                        b = x["executionFailedEventDetails"]["cause"]
                        if b != None:
                            e = re.findall(r".*\'\$\.INPUT\.([\w]+)\'.*",b)
                            temp_dict["JOB_NAME"] = e
                            #print("temp_dict b {}".format(temp_dict["JOB_NAME"]))
                        #temp_dict["JOB_NAME"] = 'NA'
                        if i != None:
                            if "startDate" in i:
                                temp_dict["START_TIME"] = convert_date_format(i["startDate"])
                                temp_dict["START_DATE"] = temp_dict["START_TIME"][0:10]
                            if "StopDate" in i:
                                temp_dict["STOP_TIME"] = convert_date_format(i["StopDate"])
                            if "ARN_status" in i:
                                temp_dict["JOB_STATUS"] = i["ARN_status"]
                            if "startDate" in i and "StopDate" in i:
                                temp_dict["START_STOP_DIFF_SEC"] = round(calc_time_difference_sec(convert_date_format(i["startDate"]), convert_date_format(i["StopDate"])))
                            if "startDate" in i and "StopDate" in i:
                                temp_dict["START_STOP_DIFF_MINS"] = round(calc_time_difference_min(convert_date_format(i["startDate"]), convert_date_format(i["StopDate"])),2)
                            if temp_dict:
                                temp_list.append(temp_dict)
        return temp_list
            
    except Exception as e:
        print("Error {}".format(e))
        
def sfn_list_executions(sfn_arn):
    try:
        filter_compaction_exe = []
        list_exe_response = sfn_client.list_executions(stateMachineArn=sfn_arn)
        #print("list result {}".format(list_exe_response))
        
        for i in list_exe_response["executions"]:
            print("hi-------------{}".format(i))
            temp = dict()
            temp["executionArn"] = i["executionArn"]
            temp["ARN_status"] = i["status"]
            temp["startDate"] = i["startDate"]
            if i["status"] == 'RUNNING':
                print("inside if--{}".format(i["status"]))
                temp["StopDate"] = '0000-00-00 00:00:00'
            else:
                temp["StopDate"] = i["stopDate"]
            filter_compaction_exe.append(temp)
        return filter_compaction_exe
    except Exception as e:
        print("SFN list error{}".format(e))

def lambda_handler(event, context):
    # TODO implement
    SFN_DETAILS = ["CD_ENTITY_SBOM"]
    #SFN_DETAILS = ["CD_ENTITY_MWS","CD_COMPACTION_SIEBEL_AMERICAS","CD_COMPACTION_PARTS_FILES","CD_COMPACTION_GLPROD2","CD_COMPACTION_GLPROD1","CD_COMPACTION_GLPROD","CD_COMPACTION_SIEBEL_AMERICAS2","CD_COMPACTION_SIEBEL_AMERICAS1","CD_COMPACTION_GLPROD3","CD_COMPACTION_SIT","CD_COMPACTION_SERVICEMAX","CD_COMPACTION_SBOM","CD_COMPACTION_SALESFORCE","CD_COMPACTION_MWS","CD_COMPACTION_MUST","CD_COMPACTION_GLA","CD_COMPACTION_FLAT_FILE","CD_COMPACTION_DRM","IB_INTEL_COMPACTION_PARTS_FILES","IB_INTEL_COMPACTION_GLA","IB_INTEL_COMPACTION_SIT","IB_INTEL_COMPACTION_SIEBEL_AMERICAS","IB_INTEL_COMPACTION_GIB","IB_INTEL_COMPACTION_FLAT_FILE","IB_INTEL_COMPACTION_SIEBEL_AMERICAS1","IB_INTEL_COMPACTION_SERVICEMAX2","IB_INTEL_COMPACTION_SIT2","IB_INTEL_COMPACTION_SIT1","IB_INTEL_COMPACTION_SIEBEL_AMERICAS2","IB_INTEL_COMPACTION_SERVICEMAX1","IB_INTEL_COMPACTION_SERVICEMAX","IB_INTEL_COMPACTION_SALESFORCE","IB_INTEL_COMPACTION_PQW","IB_INTEL_COMPACTION_MUST","IB_INTEL_COMPACTION_GLPROD","IB_INTEL_COMPACTION_CONNECTIVITY","CD_ENTITY_ONE_TIME_INSERT","CD_ENTITY_SIT","CD_ENTITY_SIEBEL_AMERICAS","CD_ENTITY_SERVICEMAX","CD_ENTITY_SBOM","CD_ENTITY_OPH","CD_ENTITY_MUST","CD_ENTITY_HR_FLAT_FILE","CD_ENTITY_GLPROD","CD_ENTITY_GLA","CD_ENTITY_DRM","CD_ENTITY_CARES","CD_ENTITY_CAPS"]
    temp_list = []
    S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    S3_FOLDER_PATH = os.environ['S3_PREFIX']
    for SFN_NAME in range(len(SFN_DETAILS)):
        SFN_ARN_NAME = "arn:aws:states:us-east-1:245792935030:stateMachine:" + str(SFN_DETAILS[SFN_NAME])
        get_exe_arn = sfn_list_executions(SFN_ARN_NAME)
        list = (sfn_get_exe_history(get_exe_arn))
        #print("list {}".format(list))
        temp_list.append(list)
    r_list = reformat_list(temp_list)
    generate_csv_report(r_list, S3_BUCKET_NAME, S3_FOLDER_PATH)
    

        
    
    
    
    
    
    
    
    
    
    
