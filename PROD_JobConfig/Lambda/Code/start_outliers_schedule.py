import json
import boto3
import os
from datetime import datetime, date
import calendar

sns_client = boto3.client('sns')
sfn_client = boto3.client('stepfunctions')


def sfn_invoke_sm(sfn_name, sfn_input):
    try:

        sfn_response = sfn_client.start_execution(
            stateMachineArn=sfn_name,
            input=json.dumps(sfn_input)
        )

        print("State machine response {}".format(sfn_response))
        return True

    except Exception as e:
        print("Not able to start step function!. Error {}".format(e))
        raise e


def sns_notification(sns_arn, msg, task_status, task_date):
    try:
        response = sns_client.publish(
            TopicArn=sns_arn,
            Message=msg,
            Subject='ODP Outliers Status ' + task_status + ' - ' + task_date
        )
        return True

    except Exception as e:
        print("Not able to send notification!. Error {}".format(e))
        return False


def lambda_handler(event, context):
    try:

        current_account_number = context.invoked_function_arn.split(":")[4]
        sns_arn = 'arn:aws:sns:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':ODP_SFN_SUCCESS'
        sfn_arn = 'arn:aws:states:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':stateMachine:' + \
                  event["SFN_NAME"]
        sfn_input = event["SFN_INPUT"]

        print("SFN ARN {}".format(sfn_arn))

        t_date = datetime.today().strftime("%d-%m-%Y")

        first_day_of_the_month = datetime.now().day

        if first_day_of_the_month == 1:

            print(first_day_of_the_month)
            msg = "Hi All, Today is month {}st day.  Running the job {}".format(first_day_of_the_month,
                                                                                'OUTLIERS_CDS_CRM_REVENUE_CDS')
            print(msg)

            sns_notification(sns_arn, msg, 'SUCCESS', t_date)
            sfn_invoke_sm(sfn_arn, sfn_input)

        else:

            dt_today = str(date.today()).split('-')
            month_last_day = calendar.monthrange(int(dt_today[0]), int(dt_today[1]))[1]
            days_remains = month_last_day - first_day_of_the_month

            sfn_input['INPUT']['OUTLIERS_CDS_CRM_REVENUE_CDS'] = 'Y'

            msg = "Skipping {} this job {} more days to go to trigger this...".format('OUTLIERS_CDS_CRM_REVENUE_CDS',
                                                                                      days_remains)
            sns_notification(sns_arn, msg, 'SUCCESS', t_date)
            sfn_invoke_sm(sfn_arn, sfn_input)


    except KeyError as ke:
        print("{} No such key!".format(ke))
    except Exception as e:
        print("Oops! error in lambda_handler.")
