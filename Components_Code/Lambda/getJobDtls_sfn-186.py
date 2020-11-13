import json
import os
from datetime import datetime
import boto3
import os

# Create client for aws service
SFN_CLIENT = boto3.client('stepfunctions')
S3_CLIENT = boto3.client('s3')
GLUE_CLIENT = boto3.client('glue')

# Declare static variables
# Get the bucket name from Environment Variable.
SRC_BUCKET_NAME = os.environ['SRC_BUCKET_NAME']


# Unpack the dictionary values
def extract_dict_values(dict_payload):
    """
    A function to extract Key and Value from dictionary
    :param dict_payload:
    :return:
    """
    return dict_payload.items()


def extract_task_def(dict_payload):
    """
    A function to filter the Glue Job names from
    the Step function.

    Scenarios:
    Extract the Job names from the given Step function state machine.
    It filter only the Glue jobs from the state machine and
    return list of identified jobs
    It support to extract One state, parallel state and
    nested parallel state with single or multiple states.


    :param dict_payload:
    :return:
    """
    jbs = []

    try:
        # Convert JSON string to JSON.
        dict_payload = json.loads(dict_payload['definition'])

        # Unpack and process the state machine.
        for inner_dict_key, inner_dict_value in extract_dict_values(dict_payload['States']):
            # Check, whether the parallel state present
            if inner_dict_value['Type'] == 'Parallel':
                # Here the loop defined to iterate the list
                for list_of_dict in inner_dict_value['Branches']:
                    for resource_key, resource_value in extract_dict_values(list_of_dict['States']):
                        # This block will check whether nested 'parallel' job is defined or not.
                        if resource_value['Type'] == 'Parallel':
                            for nested_list_of_dict in resource_value['Branches']:
                                for nrk, nrv in extract_dict_values(nested_list_of_dict['States']):
                                    try:
                                        # Identify and extract only glue jobs from state machine.
                                        if nrv['Resource'].split(':')[5] == 'glue':
                                            # Filter then append the Glue job name to the return list.
                                            jbs.append(nrv['Parameters']['JobName'])
                                    except KeyError:
                                        continue

                        # This block will execute if any state present in nested parallel section
                        elif resource_value['Type'] == 'Task':
                            try:
                                if resource_value['Resource'].split(':')[5] == 'glue':
                                    # print('{}\n{}'.format(resource_key, resource_value['Parameters']['JobName']))
                                    jbs.append(resource_value['Parameters']['JobName'])
                            except KeyError:
                                continue

            # If there is no 'parallel' states will directly start from here.
            elif inner_dict_value['Type'] == 'Task':
                try:
                    if inner_dict_value['Resource'].split(':')[5] == 'glue':
                        print('Inner job names : {}'.format(inner_dict_value['Parameters']['JobName']))
                        jbs.append(inner_dict_value['Parameters']['JobName'])
                except KeyError:
                    continue

    except Exception as task_definition_error:
        print('Error {}'.format(task_definition_error))
        return None

    return jbs


# A function to describe the Step function state machine.
def sfn_describe_sm(sfn_arn):
    """
    Describe the Step function

    :param sfn_arn:
    :return:
    """
    try:
        describe_sfn_response = SFN_CLIENT.describe_state_machine(stateMachineArn=sfn_arn)
        # print("State description response : {}".format(describe_sfn_response))
        return describe_sfn_response

    except Exception as sfn_def_error:
        print('Error while describe step function state machine {}'.format(sfn_def_error))
        return None


# A function to list and get the latest file from S3 using the prefix path given by user.
def retrieve_latest_s3_file(sfn_time_st, glue_job_name, bucket_prefix):
    """
    A function to retrieve the timestamp from latest file from S3 and
    and compare with the last successful glue job time.

    :param sfn_time_st:
    :param glue_job_name:
    :param bucket_prefix:
    :return:
    """
    # Create a temporary variable
    s3_file_timestamp_dict = {}

    # Extract the DAY, MONTH, YEAR & HOUR for Partition
    p_year, p_month, p_day = sfn_time_st.strftime('%Y-%m-%d').split('-')
    # p_hour = sfn_time_st.strftime('%H')

    # Append YEAR partition along with the given input prefix.
    formatted_prefix = bucket_prefix + 'year=' + str(p_year)

    print('Partition {}'.format(formatted_prefix))

    try:
        # Create S3 client
        # s3_response = S3_CLIENT.list_objects_v2(Bucket=SRC_BUCKET_NAME, Prefix=formatted_prefix)
        # list_of_keys = s3_response['Contents']
        # # Get the latest file using MAX operation
        # latest_file = max(list_of_keys, key=lambda x: x['LastModified'])
        # latest_file_timestamp = datetime.strptime(str(latest_file['LastModified']), '%Y-%m-%d %H:%M:%S+00:00')
        complete_page = S3_CLIENT.get_paginator('list_objects_v2')
        s3_response = complete_page.paginate(Bucket=SRC_BUCKET_NAME, Prefix=formatted_prefix)
        latest_file = ''
        latest_file_timestamp = ''

        for rsp in s3_response:
            # Get the latest file using MAX operation
            latest_file = max(rsp['Contents'], key=lambda x: x['LastModified'])
            latest_file_timestamp = datetime.strptime(str(latest_file['LastModified']), '%Y-%m-%d %H:%M:%S+00:00')

        print('S3 Response latest file {} and time {}'.format(latest_file['Key'], latest_file_timestamp))

        time_difference_in_seconds = find_time_diff(sfn_time_st, latest_file_timestamp)
        print('Time difference {}'.format(time_difference_in_seconds))

        # Set the flag for the Job
        if time_difference_in_seconds < 0:
            s3_file_timestamp_dict.update({glue_job_name: 'N'})

        else:
            # Job will SKIP
            s3_file_timestamp_dict.update({glue_job_name: 'Y'})

        return s3_file_timestamp_dict

    except Exception as partition_error:

        print('No such partition! {}'.format(partition_error))
        s3_file_timestamp_dict.update({glue_job_name: 'N'})

        return s3_file_timestamp_dict


# A function to calculate difference between two timestamp
def find_time_diff(glue_timest, s3_timest):
    """
    A function to get the difference between two timestamp.

    :param glue_timest:
    :param s3_timest:
    :return:
    """
    try:

        time_difference = glue_timest - s3_timest
        time_diff_in_secs = time_difference.total_seconds()

        print('Glue Time : {}, S3 File time {}'.format(glue_timest, s3_timest))

        return time_diff_in_secs

    except Exception as time_exception:
        print('Invalid time format {}'.format(time_exception))
        return None


# A function to check the Bookmark is enabled for the given Glue Job.
def glue_jbk_history(glue_job_name):
    """
    A function to check whether the

    :param glue_job_name:
    :return:
    """
    try:
        glue_jbk_response = GLUE_CLIENT.get_job_bookmark(JobName=glue_job_name)
        # If the bookmark enabled, will get the RUN_ID for the job.
        return glue_jbk_response['JobBookmarkEntry']['RunId']

    except Exception as glue_error:
        print('glue error {}'.format(glue_error))
        return None


# A function to the start time from the successful job history
def get_job_history(glue_job_name, glue_job_run_id):
    """
    A function to retrieve the Job RUN_ID from job history.
    :param glue_job_name:
    :param glue_job_run_id:
    :return:
    """

    try:
        print('RUN ID {}'.format(glue_job_run_id))
        history_response = GLUE_CLIENT.get_job_run(JobName=glue_job_name, RunId=glue_job_run_id,
                                                   PredecessorsIncluded=True)
        return history_response

    except Exception as history_error:
        print('Get history Error {}'.format(history_error))
        return None


# A main funciton start here...
def lambda_handler(event, context):
    # Response JSON variable
    lambda_response = {}
    check_list = []

    # To check whether the Input Payload is empty.
    check_event = not event

    if not check_event:

        try:
            # Construct step function ARN from the given Name.
            current_lambda_account = context.invoked_function_arn.split(":")[4]
            sfn_arn = 'arn:aws:states:' + os.environ['AWS_REGION'] + ':' + current_lambda_account + ':stateMachine:' + \
                      event['STEPFUNCTION_NAME']

            for jobs_key, jobs_value in extract_dict_values(event['JOBS']):
                check_list.append(jobs_key)

            # Collect the 'definition' to process further.
            sfn_response_payload = sfn_describe_sm(sfn_arn)

            if sfn_response_payload is not None:

                list_of_jobs = extract_task_def(sfn_response_payload)
                print('List of Glue Jobs {}'.format(list_of_jobs))

                try:
                    for skip_flag_job_key, skip_flag_job_value in extract_dict_values(event['SKIP_FLAG']):
                        if skip_flag_job_value == 'Y':
                            lambda_response.update({skip_flag_job_key: skip_flag_job_value})
                        else:
                            if skip_flag_job_key in check_list:

                                if skip_flag_job_key in list_of_jobs:

                                    for job_name, s3_prefix in event['JOBS'].items():
                                        print('Job input check with path {}'.format(job_name))
                                        # Check whether the User input job is available in step function state machine task list
                                        if job_name == skip_flag_job_key:

                                            job_run_id = glue_jbk_history(job_name)

                                            if job_run_id is not None:

                                                job_history = get_job_history(job_name, job_run_id)

                                                if job_history is not None:
                                                    # Extract the JOB_START_TIME from JOB_History response
                                                    job_start_time = datetime.strptime(
                                                        str(job_history["JobRun"]["StartedOn"]),
                                                        '%Y-%m-%d %H:%M:%S.%f+00:00')

                                                    s3_file_response = retrieve_latest_s3_file(job_start_time, job_name,
                                                                                               s3_prefix)
                                                    print('S3 file response {}'.format(s3_file_response))

                                                    # update the lambda response value
                                                    lambda_response.update(s3_file_response)
                                            else:
                                                print('Run id None job {}'.format(job_name))
                                                lambda_response.update({job_name: 'N'})
                                else:
                                    lambda_response.update({skip_flag_job_key: skip_flag_job_value})
                            else:
                                lambda_response.update({skip_flag_job_key: skip_flag_job_value})

                    return lambda_response

                except Exception as job_name_error:
                    print('Error while check job name with user input {}'.format(job_name_error))

        except Exception as job_error:

            print('Error in input job list {}'.format(job_error))
            return {'Error': 'Invalid list objects'}

    else:
        return {'Error': 'Input payload empty'}