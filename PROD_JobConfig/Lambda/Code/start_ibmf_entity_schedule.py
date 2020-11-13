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

        if kwargs['partKey'] == "IBMF-Entity":
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
            Subject='ODP IBMF Entity LOAD Status ' + task_status + ' - ' + task_date
        )
        return True

    except Exception as e:
        print("Not able to send notification!. Error {}".format(e))
        return False


def reset_flag(**user_flag_flip):
    temp_dict = {}
    for k, v in user_flag_flip.items():
        temp_dict.update({k: v})
        if v == "N":
            temp_dict.update({k: "Y"})
    return temp_dict


def main(TRACK_NAME, SFN_ARN, SFN_NAME, SFN_INPUT, SNS_ARN):
    try:
        # Check whether the entry present in Dynamodb
        validate_date_repsonse = validate_date()
        print("v respnose {}".format(validate_date_repsonse))

        if validate_date_repsonse:
            try:

                ibmf_partKey, ibmf_sortKey = 'IBMF-Entity', validate_date_repsonse

                print("sp {}, sk {}".format(ibmf_partKey, ibmf_sortKey))

                ddb_second_response = ddb_get_unique_record(ibmf_partKey, ibmf_sortKey)

                if ddb_second_response:
                    print("Second response {}".format(ddb_second_response))

                    ddb_default_data = {
                        "partKey": ddb_second_response['partKey'],
                        "sortKey": ddb_second_response['sortKey'],
                        "SR_CDS": ddb_second_response['SR_CDS'],
                        "MAPP_ENTITY": ddb_second_response['MAPP_ENTITY']
                    }

                    check_n_count = list(filter(lambda filter_n: filter_n == 'N', ddb_second_response.values()))
                    print("Total N count : {}".format(len(check_n_count)))

                    if len(check_n_count) == 1:

                        print("Track {} and length {}".format(TRACK_NAME, len(check_n_count)))
                        print("Inside if loop {}".format(ddb_default_data))
                        ddb_default_data[TRACK_NAME] = "Y"
                        ddb_update_records(**ddb_default_data)
                        print("IF BLOCK {}".format(ddb_default_data))

                        dt_now = datetime.now()
                        week_of_the_day = dt_now.strftime("%A")

                        if week_of_the_day == "Monday":

                            print("{} is {}".format(dt_now, week_of_the_day))
                            print("{}".format(SFN_INPUT['INPUT']))

                        elif week_of_the_day == "Saturday" or week_of_the_day == "Sunday":

                            print("{} is {}".format(dt_now, week_of_the_day))
                            SFN_INPUT['INPUT'] = reset_flag(**SFN_INPUT['INPUT'])

                        else:

                            SFN_INPUT['INPUT']['IBL_SHIPMENT_METRICS_FINAL_SNAP_F'] = 'Y'
                            SFN_INPUT['INPUT']['IBL_SIEBEL_ASSET_COMPLETENESS_LOAD_WEEKLY'] = 'Y'
                            print("Week of the day is {}".format(week_of_the_day))

                        no_of_jobs = list(filter(lambda filter_n: filter_n == 'N', SFN_INPUT['INPUT'].values()))

                        msg = "SR CDS & MAPP Entity loads are completed and starting NONSS IBMF Entity / CDS, {} jobs executed on {}".format(
                            len(no_of_jobs), week_of_the_day)
                        print(msg)

                        sns_notification(SNS_ARN, msg, "SUCCESS", validate_date_repsonse)
                        sfn_invoke_sm(SFN_ARN, SFN_INPUT)

                    else:
                        print("Only {} going to update".format(TRACK_NAME))

                        msg = "{} only completed yet to start NONSS IBMF Entity / CDS".format(TRACK_NAME)
                        print(msg)
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
