import json
from datetime import datetime, timedelta
import boto3
import os

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
            Subject='ODP Rule Studio Status ' + task_status + ' - ' + task_date
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
        sfn_input = event['SFN_INPUT']

        print("SFN ARN {}".format(sfn_arn))

        notification_date = datetime.today().strftime("%d-%m-%Y")

        date_today = datetime.now()
        print("Today : {}".format(date_today))

        week_of_the_day = date_today.strftime("%A")

        if week_of_the_day == "Sunday":
            msg = "Hi All, Today is " + week_of_the_day + " starting RULE STUDIO."
            print(msg)
            sns_notification(sns_arn, msg, 'SUCCESS', notification_date)
            sfn_invoke_sm(sfn_arn, sfn_input)

        else:
            msg = "Hi All, RULE STUDIO is NOT started today. Because, " + week_of_the_day + " today!"
            print(msg)
            sns_notification(sns_arn, msg, 'ALERT', notification_date)

    except KeyError as ke:
        print("{} No such key!".format(ke))
    except Exception as e:
        print("Oops! error in lambda_handler.")
