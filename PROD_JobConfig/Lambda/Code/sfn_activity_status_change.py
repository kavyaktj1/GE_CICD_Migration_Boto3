import os
import json
import boto3


SFN_CLIENT = boto3.client('stepfunctions')


def get_activity_token(activity_arn, worker_name):

    try:

        get_token = SFN_CLIENT.get_activity_task(activityArn=activity_arn, workerName=worker_name)
        return get_token['taskToken']

    except Exception as e:
        print('Get token error {}'.format(e))
        return None


def send_success_notification(task_token):

    try:
        fmt_dict = {}
        fmt_dict.update({"TASK_STATUS": "Success"})
        ss_notification = SFN_CLIENT.send_task_success(taskToken=task_token, output=json.dumps(fmt_dict))
        return ss_notification

    except Exception as e:
        print('Unable to send success notification {}'.format(e))
        return None


def send_fail_notification(task_token, error_msg, cause_msg):

    try:

        sf_notification = SFN_CLIENT.send_task_failure(taskToken=task_token, error=error_msg, cause=cause_msg)
        return sf_notification

    except Exception as e:
        print('Unable to send fail notification {}'.format(e))
        return None


def lambda_handler(event, context):

    CURRENT_ACCOUNT_NUMBER = context.invoked_function_arn.split(":")[4]
    CURRENT_FUNCTION_NAME = context.function_name

    try:
        TASK_STATUS = event['STATUS']
        TASK_ERROR = event['ERROR']
        TASK_ERROR_CAUSE = event['CAUSE']
        TASK_MANUAL_TOKEN = event['TOKEN']

        ACTIVITY_ARN = 'arn:aws:states:' + os.environ['AWS_REGION'] + ':' + CURRENT_ACCOUNT_NUMBER + ':activity:' + \
                  event['ACTIVITY_NAME']
        
        if TASK_MANUAL_TOKEN != "":
            ACT_TOKEN = TASK_MANUAL_TOKEN
            print("Manual token {}".format(ACT_TOKEN))
        
        else:
            ACT_TOKEN = get_activity_token(ACTIVITY_ARN, CURRENT_FUNCTION_NAME)
            print('Activity Token : {}'.format(ACT_TOKEN))
        
        if ACT_TOKEN is not None:
        
            if TASK_STATUS == 'SUCCESS':
                send_success_notification(ACT_TOKEN)

            elif TASK_STATUS == 'FAILED':
                # if TASK_ERROR == "" or TASK_ERROR_CAUSE = "":
                #     TASK_ERROR = TASK_STATUS
                #     TASK_ERROR_CAUSE = ""
                send_fail_notification(ACT_TOKEN, TASK_ERROR, TASK_ERROR_CAUSE)

            else:
                print('Invalid status, please provide either {} or {} in TASK_STATUS'.format('SUCCESS', 'FAILED'))

        else:
            print('Unable to get the task token')

    except KeyError:
        print('Invalid Key input')
        pass

    except Exception as e:
        print('Something went wrong! {}'.format(e))
