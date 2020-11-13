import json
import boto3
import csv
from datetime import datetime
import os
import datetime
import dateutil.tz

GLUE_CLIENT = boto3.client('glue')
S3_CLINET = boto3.client('s3')


def convert_date_format(in_date):
    tz_ist = dateutil.tz.gettz('Asia/Kolkata')
    dt_fmt = in_date.replace(tzinfo=tz_ist).strftime('%Y-%m-%d %H:%M:%S')
    return dt_fmt


def upload_report_s3(jobs_list, bucket_name, prefix_path):
    try:
        temp_file_name = '/tmp/output.csv'
        # print(jobs_list)
        rep_name = "glue_job_report_" + str(datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")) + ".csv"
        target_path = prefix_path + rep_name
        print("Target path : {}".format(target_path))

        kys = jobs_list[0].keys()
        print("Keys {}".format(kys))

        with open(temp_file_name, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, kys)
            dict_writer.writeheader()
            dict_writer.writerows(jobs_list)
        
        # with open('/tmp/output.csv', 'r') as f:
        #     print(f.read())

        S3_CLINET.upload_file(temp_file_name, bucket_name, target_path)

    except Exception as e:
        print("Error while upload report to S3 {} ".format(e))


def get_start_stop_time(glue_job_name):
    try:
        # print("Check time")
        glue_job_response = GLUE_CLIENT.get_job_runs(JobName=glue_job_name)
        temp_dict = {}

        for i in glue_job_response["JobRuns"]:
            if i["JobRunState"] == "SUCCEEDED":
                # print("Status {} Start {} Stop {}".format(i["JobRunState"], convert_time_stamp(i["StartedOn"]),convert_time_stamp(i["CompletedOn"])))
                temp_dict.update({"Job Name": i["JobName"]})
                temp_dict.update({"Job RunState": i["JobRunState"]})
                temp_dict.update({"Job Started": convert_date_format(i["StartedOn"])})
                temp_dict.update({"Job Completed": convert_date_format(i["CompletedOn"])})
                temp_dict.update({"Job Execution Time": i["ExecutionTime"]})

                break

        print("Latest Run Time {}".format(temp_dict))
        return temp_dict

    except Exception as e:
        print("Error {}".format(e))


def get_glue_job_details():
    jobs_details_list = []
    job_count = 0
    page_count = 0
    check_job_list = []

    jobs_reponse = GLUE_CLIENT.get_jobs()
    print(jobs_reponse['NextToken'])
    paginator = GLUE_CLIENT.get_paginator('get_jobs')
    glue_job_response = paginator.paginate(NextToken=jobs_reponse['NextToken'])

    # t_dict = {}

    for x in jobs_reponse['Jobs']:
        job_count += 1

        try:
            t_dict = {}
            check_job_list.append(x['Name'])

            JOB_NAME = x['Name']
            JOB_ROLE = x['Role']
            JOB_CREATED = convert_date_format(x['CreatedOn'])
            JOB_LAST_MODIFIED = convert_date_format(x['LastModifiedOn'])
            JOB_SCRIPT_LOCATION = x['Command']['ScriptLocation']
            t_dict.update({"Job Name": JOB_NAME})
            t_dict.update({"Role": JOB_ROLE})
            t_dict.update({"Created Time": JOB_CREATED})
            t_dict.update({"Modified Time": JOB_LAST_MODIFIED})
            t_dict.update({"Script Location": JOB_SCRIPT_LOCATION})
            t_dict.update(get_start_stop_time(x['Name']))

            jobs_details_list.append(t_dict)


        except KeyError:
            print("Key error")
            continue

        except Exception as e:
            print("Error {}".format(e))

    for j in glue_job_response:
        page_count += 1

        for i in j['Jobs']:
            job_count += 1

            try:
                t_dict = {}
                check_job_list.append(i['Name'])

                if "_ov_" in i['Name'] or "_no_ov_" in i["Name"] or "job_lnd_mrr_full_refresh_" in i["Name"]:
                    JOB_NAME = i['Name']
                    JOB_ROLE = i['Role']
                    JOB_CREATED = convert_date_format(i['CreatedOn'])
                    JOB_LAST_MODIFIED = convert_date_format(i['LastModifiedOn'])
                    JOB_SCRIPT_LOCATION = i['Command']['ScriptLocation']

                    t_dict.update({"Job Name": JOB_NAME})
                    t_dict.update({"Role": JOB_ROLE})
                    t_dict.update({"Created Time": JOB_CREATED})
                    t_dict.update({"Modified Time": JOB_LAST_MODIFIED})
                    t_dict.update({"Script Location": JOB_SCRIPT_LOCATION})

                    jobs_details_list.append(t_dict)


            except KeyError:
                print("Key error")
                continue

            except Exception as e:
                print("Error {}".format(e))

    print("Names {} Len {}".format(check_job_list, len(check_job_list)))
    print("Page {} Record {}".format(page_count, job_count))

    return jobs_details_list


def lambda_handler(event, context):
    print("main starts here...")
    try:
        bucket_name = os.environ['BUCKET_NAME']
        prefix_path = "servicesuite/logs/"

        jobs_list = get_glue_job_details()

        upload_report_s3(jobs_list, bucket_name, prefix_path)
        return {"Status": "Report Generated"}
    except Exception as e:
        print("Error in lambda {}".format(e))
        raise e