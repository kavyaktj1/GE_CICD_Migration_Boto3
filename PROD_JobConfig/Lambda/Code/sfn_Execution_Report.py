import json
import datetime
import csv
import os
import boto3

sfn_client = boto3.client("stepfunctions")
S3_CLINET = boto3.client('s3')

def reformat_list(list_val):
    r_list = []
    if list_val != '' and list_val is not None :
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
            
        header = ["Source System","Schedule Name","Schedule ARN No","Date","Start time","End time","Duration_Min","Execution Status"]
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




def convert_date_format(in_date):
    dt_fmt = in_date.strftime('%Y-%m-%d %H:%M:%S')
    return dt_fmt


def calc_time_difference_min(dt_one, dt_two):
    start_dt = datetime.datetime.strptime(str(dt_one), '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.datetime.strptime(str(dt_two), '%Y-%m-%d %H:%M:%S')

    time_difference = end_dt - start_dt
    time_difference_mins = (time_difference.total_seconds() / 60)

    return time_difference_mins
    
'''def calc_time_difference_sec(dt_one, dt_two):
    start_dt = datetime.datetime.strptime(str(dt_one), '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.datetime.strptime(str(dt_two), '%Y-%m-%d %H:%M:%S')

    time_difference = end_dt - start_dt
    time_difference_sec = (time_difference.total_seconds())

    return time_difference_sec'''

        
def sfn_list_executions(sfn_arn):
    try:
        filter_compaction_exe = []
        list_exe_response = sfn_client.list_executions(stateMachineArn=sfn_arn)
        #print("list result {}".format(list_exe_response))
        
        for i in list_exe_response["executions"]:
            #print("hi-------------{}".format(i))
            temp = dict()
            split_data = i["executionArn"].split(":")
            temp["Source System"] = ''
            temp["Schedule Name"] = split_data[6]
            split_Schedule_name = temp["Schedule Name"].split("_")
            temp["Source System"] = split_Schedule_name[0]
            temp["Schedule ARN No"] = split_data[7]
            temp["Date"] = ''
            temp["Start time"] = convert_date_format(i["startDate"])
            if i["status"] == 'RUNNING':
                print("inside if--{}".format(i["status"]))
                temp["End time"] = '0000-00-00 00:00:00'
            else:
                temp["End time"] = convert_date_format(i["stopDate"])
            temp["Duration_Min"] = round(calc_time_difference_min(temp["Start time"],temp["End time"]),2)
            temp["Date"] = temp["Start time"][0:10]
            temp["Execution Status"] = i["status"]
            filter_compaction_exe.append(temp)
        return filter_compaction_exe
    except Exception as e:
        print("SFN list error{}".format(e))

def lambda_handler(event, context):
    # TODO implement
    SFN_DETAILS = ["GLPROD_COMPACTION","SIEBEL_AMERICAS_COMPACTION","PQW_COMPACTION","SIT_COMPACTION","DRM_COMPACTION","FLAT_FILE_COMPACTION","SBOM_COMPACTION","SERVICEMAX_COMPACTION","SALESFORCE_COMPACTION","GIB_COMPACTION","MUST_COMPACTION","GLA_COMPACTION","MWS_COMPACTION","CD_ENTITY_MASTER_SCHEDULE","IB_INTEL_ENTITY_MASTER_SCHEDULE","IB_INTEL_CDS_MASTER_SCHEDULE"]
    temp_list = []
    S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    S3_PREFIX = os.environ['S3_PREFIX']
    for SFN_NAME in range(len(SFN_DETAILS)):
        SFN_ARN_NAME = "arn:aws:states:us-east-1:541574621075:stateMachine:" + str(SFN_DETAILS[SFN_NAME])
        get_exe_arn = sfn_list_executions(SFN_ARN_NAME)
        if get_exe_arn is not None:
            list = [x for x in get_exe_arn if x]
            temp_list.append(list)
    r_list = reformat_list(temp_list)
    #print("r_list==================== {}".format(r_list))
    generate_csv_report(r_list, S3_BUCKET_NAME, S3_PREFIX)
    

        
    
    
    
    
    
    
    
    
    
    
