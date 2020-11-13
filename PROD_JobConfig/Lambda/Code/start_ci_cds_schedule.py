import json
from datetime import datetime, timedelta
import boto3
import os

DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

sns_client = boto3.client('sns')
sfn_client = boto3.client('stepfunctions')

date_today = datetime.today().strftime("%d-%m-%Y")
date_yesterday = (datetime.today() - timedelta(1)).strftime("%d-%m-%Y")

print("Today : {}".format(date_today))


def ddb_get_unique_record(partKey, sortKey):
    try:
        format_key = {
            "partKey": partKey,
            "sortKey": sortKey
        }

        ddb_get_response = ddb_table.get_item(Key=format_key)
        print("Get response {}".format(ddb_get_response))
        return ddb_get_response["Item"]

    except Exception as e:
        print("Get Error {}".format(e))
        return False


def validate_date():
    try:

        first_partKey, first_sortKey = "ODP-Daily", "Compaction"
        ddb_first_response = ddb_get_unique_record(first_partKey, first_sortKey)

        if (ddb_first_response["CURRENT_EXE_DATE"] == date_today) or (
                ddb_first_response["CURRENT_EXE_DATE"] == date_yesterday):
            return ddb_first_response["CURRENT_EXE_DATE"]

    except Exception as e:
        print("Validate Error {}".format(e))
        return False

def ddb_update_records(**kwargs):
    try:
        default_data = {}
        print('ddb default format {}'.format(json.dumps(kwargs, indent=4)))

        if kwargs['partKey'] == "CI-CDS":
            print("Part key {}".format(kwargs['partKey']))
            ddb_put_response = ddb_table.put_item(Item=kwargs)
            print("Insert {}".format(ddb_put_response))

        return True

    except Exception as e:
        print("Put error {}".format(e))
        return False


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
            Subject='ODP SR CDS LOAD Status ' + task_status + ' - ' + task_date
        )
        return True

    except Exception as e:
        print("Not able to send notification!. Error {}".format(e))
        return False


def main(TRACK_NAME, SFN_ARN, SFN_NAME, SFN_INPUT, SNS_ARN):
    try:
        # Check whether the entry present in Dynamodb
        validate_date_repsonse = validate_date()
        print("v respnose {}".format(validate_date_repsonse))

        if validate_date_repsonse:
            try:

                ci_cds_partKey, ci_cds_sortKey = 'CI-CDS', validate_date_repsonse

                print("sp {}, sk {}".format(ci_cds_partKey, ci_cds_sortKey))

                ddb_second_response = ddb_get_unique_record(ci_cds_partKey, ci_cds_sortKey)

                if ddb_second_response:
                    print("Second response {}".format(ddb_second_response))

                    ddb_default_data = {
                        "partKey": ddb_second_response['partKey'],
                        "sortKey": ddb_second_response['sortKey'],
                        "IB_INTEL_CDS": ddb_second_response['IB_INTEL_CDS'],
                        "CI_ENTITY": ddb_second_response['CI_ENTITY']
                    }

                    check_n_count = list(filter(lambda filter_n: filter_n == 'N', ddb_second_response.values()))
                    print("Total N count : {}".format(len(check_n_count)))

                    if len(check_n_count) == 1:

                        print("Track {} and length {}".format(TRACK_NAME, len(check_n_count)))
                        print("Inside if loop {}".format(ddb_default_data))
                        ddb_default_data[TRACK_NAME] = "Y"
                        ddb_update_records(**ddb_default_data)
                        print("IF BLOCK {}".format(ddb_default_data))

                        msg = "CI Entity & IB INTEL CDS loads are completed and starting CI CDS Load"
                        sns_notification(SNS_ARN, msg, "SUCCESS", validate_date_repsonse)
                        sfn_invoke_sm(SFN_ARN, SFN_INPUT)

                    else:
                        print("Only {} going to update".format(TRACK_NAME))

                        msg = "{} only completed yet to start CI CDS Load".format(TRACK_NAME)
                        sns_notification(SNS_ARN, msg, "UPDATED", validate_date_repsonse)

                        ddb_default_data.update({TRACK_NAME: "Y"})
                        ddb_update_records(**ddb_default_data)
                        print("Else block {}".format(ddb_default_data))

            except Exception as e:
                print("Error while getting second ddb response {}".format(e))

        else:
            print("New date {}".format(validate_date))

    except Exception as e:
        print("Error in main {}".format(e))


def lambda_handler(event, context):
    try:
        current_account_number = context.invoked_function_arn.split(":")[4]
        sns_arn = 'arn:aws:sns:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':ODP_SFN_SUCCESS'
        sfn_arn = 'arn:aws:states:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':stateMachine:' + \
                  event["SFN_NAME"]
        print("SFN ARN {}".format(sfn_arn))

        TRACK_NAME = event["TRACK_NAME"]

        main(TRACK_NAME, sfn_arn, event["SFN_NAME"], event["SFN_INPUT"], sns_arn)

    except KeyError as ke:
        print("{} No such key!".format(ke))
    except Exception as e:
        print("Oops! error in lambda_handler.")
