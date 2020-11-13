import json
from datetime import datetime, timedelta
import boto3
import os
from botocore.exceptions import ClientError

DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

sns_client = boto3.client('sns')
sfn_client = boto3.client('stepfunctions')

date_today = datetime.today().strftime("%d-%m-%Y")
date_yesterday = (datetime.today() - timedelta(1)).strftime("%d-%m-%Y")

print("Today's Date : {}".format(date_today))
print("Yesterday's Date : {}".format(date_yesterday))


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
        print("PartKey {}".format(kwargs['partKey']))

        default_data = {}

        if kwargs['partKey'] == "Compaction":
            default_data = kwargs

        elif kwargs['partKey'] == "ODP-Daily" or \
                kwargs['partKey'] == "SR-Entity" or \
                kwargs['partKey'] == "DQ-Framework" or \
                kwargs['partKey'] == "CI-Summary" or \
                kwargs['partKey'] == "ECOM-Daily" or \
                kwargs['partKey'] == "CI-CDS" or \
                kwargs['partKey'] == "IBMF-Entity" or \
                kwargs['partKey'] == "NPS-Spotfire":

            default_data = kwargs

        print('format {}'.format(default_data))
        ddb_put_response = ddb_table.put_item(Item=default_data)
        print("Insert {}".format(ddb_put_response))

        return True

    except Exception as e:
        print("Put error {}".format(e))
        return False


def sns_notification(sns_arn, msg, task_status, task_date):
    try:
        response = sns_client.publish(
            TopicArn=sns_arn,
            Message=msg,
            Subject='ODP Scheduled Lambda ' + task_status + ' - ' + task_date
        )
        return True

    except Exception as e:
        print("Not able to send notification!. Error {}".format(e))
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


def reset_flag(**ddb_sec_resp):
    temp_dict = {}

    for k, v in ddb_sec_resp.items():
        temp_dict.update({k: v})
        if v == "Y":
            temp_dict.update({k: "N"})

    return temp_dict


class customFail(Exception):
    pass


def main(sfn_arn, sfn_name, sfn_input, sns_arn):
    try:

        # Check whether the entry present in Dynamodb
        validate_date_repsonse = validate_date()
        print("v respnose {}".format(validate_date_repsonse))

        if validate_date_repsonse:
            try:
                second_partKey, second_sortKey = "Compaction", validate_date_repsonse
                print("sp {}, sk {}".format(second_partKey, second_sortKey))

                sr_entity_partKey, sr_entity_sortKey = "SR-Entity", validate_date_repsonse
                dqf_partKey, dqf_sortKey = "DQ-Framework", validate_date_repsonse
                ci_summ_partKey, ci_summ_sortKey = "CI-Summary", validate_date_repsonse
                ecom_partKey, ecom_sortKey = "ECOM-Daily", validate_date_repsonse
                ci_cds_partKey, ci_cds_sortKey = "CI-CDS", validate_date_repsonse
                ibmf_partKey, ibmf_sortKey = "IBMF-Entity", validate_date_repsonse
                nps_partKey, nps_sortKey = "NPS-Spotfire", validate_date_repsonse


                ddb_second_response = ddb_get_unique_record(second_partKey, second_sortKey)
                ddb_sr_entity_response = ddb_get_unique_record(sr_entity_partKey, sr_entity_sortKey)
                ddb_dqf_response = ddb_get_unique_record(dqf_partKey, dqf_sortKey)
                ddb_ci_summ_response = ddb_get_unique_record(ci_summ_partKey, ci_summ_sortKey)
                ddb_ecom_response = ddb_get_unique_record(ecom_partKey, ecom_sortKey)
                ddb_ci_cds_response = ddb_get_unique_record(ci_cds_partKey, ci_cds_sortKey)
                ddb_ibmf_response = ddb_get_unique_record(ibmf_partKey, ibmf_sortKey)
                ddb_nps_response = ddb_get_unique_record(nps_partKey, nps_sortKey)

                if ddb_second_response:
                    print("Second response {}".format(ddb_second_response))

                    check_n_count = list(filter(lambda filter_n: filter_n == 'N', ddb_second_response.values()))
                    print("Total N count : {}".format(len(check_n_count)))

                    compaction_entry = reset_flag(**ddb_second_response)

                    sr_entity_reset_flag = reset_flag(**ddb_sr_entity_response)
                    dqf_entry_reset_flag = reset_flag(**ddb_dqf_response)
                    ci_entry_reset_flag = reset_flag(**ddb_ci_summ_response)
                    ecom_entry_reset_flag = reset_flag(**ddb_ecom_response)
                    ci_cds_reset_flag = reset_flag(**ddb_ci_cds_response)
                    ibmf_reset_flag = reset_flag(**ddb_ibmf_response)
                    nps_reset_flag = reset_flag(**ddb_nps_response)

                    if len(check_n_count) == 0 and ddb_second_response["EXE_COMPLETE"] == "Y":
                        print("All completed")
                        compaction_entry["sortKey"] = date_today
                        compaction_entry["EXE_START_STATUS"] = "Y"
                        compaction_entry["START_TIME"] = str(datetime.now())

                        # Reset the CURRENT_EXE_DATE

                        daily_entry = {"partKey": "ODP-Daily", "sortKey": "Compaction", "CURRENT_EXE_DATE": date_today}
                        sr_entity_entry = {"partKey": "ODP-Daily", "sortKey": "SR-Entity",
                                           "CURRENT_EXE_DATE": date_today}
                        dqf_entry = {"partKey": "ODP-Daily", "sortKey": "DQ-Framework", "CURRENT_EXE_DATE": date_today}
                        ci_entry = {"partKey": "ODP-Daily", "sortKey": "CI-Summary", "CURRENT_EXE_DATE": date_today}
                        ecom_entry = {"partKey": "ODP-Daily", "sortKey": "ECOM-Daily", "CURRENT_EXE_DATE": date_today}
                        ci_cds_entry = {"partKey": "ODP-Daily", "sortKey": "CI-CDS", "CURRENT_EXE_DATE": date_today}
                        ibmf_entry = {"partKey": "ODP-Daily", "sortKey": "IBMF-Entity", "CURRENT_EXE_DATE": date_today}
                        nps_entry = {"partKey": "ODP-Daily", "sortKey": "NPS-Spotfire", "CURRENT_EXE_DATE": date_today}

                        ddb_update_records(**compaction_entry)
                        ddb_update_records(**daily_entry)
                        ddb_update_records(**sr_entity_entry)
                        ddb_update_records(**dqf_entry)
                        ddb_update_records(**ci_entry)
                        ddb_update_records(**ecom_entry)
                        ddb_update_records(**ci_cds_entry)
                        ddb_update_records(**ibmf_entry)
                        ddb_update_records(**nps_entry)

                        sr_entity_reset_flag["sortKey"] = date_today
                        dqf_entry_reset_flag["sortKey"] = date_today
                        ci_entry_reset_flag["sortKey"] = date_today
                        ecom_entry_reset_flag["sortKey"] = date_today
                        ci_cds_reset_flag["sortKey"] = date_today
                        ibmf_reset_flag["sortKey"] = date_today
                        nps_reset_flag["sortKey"] = date_today

                        ddb_update_records(**sr_entity_reset_flag)
                        ddb_update_records(**dqf_entry_reset_flag)
                        ddb_update_records(**ci_entry_reset_flag)
                        ddb_update_records(**ecom_entry_reset_flag)
                        ddb_update_records(**ci_cds_reset_flag)
                        ddb_update_records(**ibmf_reset_flag)
                        ddb_update_records(**nps_reset_flag)

                        msg = "The " + validate_date_repsonse + " day " + second_partKey + " jobs are completed successfully.  Starting " + sfn_name
                        sns_notification(sns_arn, msg, "SUCCESS", validate_date_repsonse)
                        sfn_invoke_sm(sfn_arn, sfn_input)

                    else:
                        print("Not completed")
                        msg = "The " + validate_date_repsonse + " day " + second_partKey + " jobs are not completed.  Please check and fix the errors!"
                        sns_notification(sns_arn, msg, "FAILED", validate_date_repsonse)

                else:
                    print("False... Inserting default values")
                    raise customFail("default value not found in DDB")

            except Exception as e:
                print("Error while getting second ddb response {}".format(e))
                raise customFail(str(e))

        else:

            print("New date {}".format(validate_date))
            raise customFail("New date error")

    except Exception as e:
        print("Error in main {}".format(e))
        raise customFail(str(e))


def lambda_handler(event, context):
    try:
        current_account_number = context.invoked_function_arn.split(":")[4]
        sns_arn = 'arn:aws:sns:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':ODP_SFN_SUCCESS'
        sfn_arn = 'arn:aws:states:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':stateMachine:' + \
                  event["SFN_NAME"]
        print("SFN ARN {}".format(sfn_arn))

        main(sfn_arn, event["SFN_NAME"], event["SFN_INPUT"], sns_arn)
        return {"Status": "Success"}

    except KeyError as ke:
        print(" {} No such key!".format(ke))
        return {"Status": "Failed with KeyError"}

    except Exception as e:
        print("Error at lambda_handler {}".format(e))
        return {"Status": "Failed"}