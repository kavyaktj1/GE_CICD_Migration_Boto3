import json
from datetime import datetime, timedelta
import boto3
import os

DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

sns_client = boto3.client('sns')

date_today = datetime.today().strftime("%d-%m-%Y")
date_yesterday = (datetime.today() - timedelta(1)).strftime("%d-%m-%Y")

print("Today's Date : {}".format(date_today))
print("Yesterday Date : {}".format(date_yesterday))


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


def ddb_update_records(**kwargs):
    try:
        print("PartKey {}".format(kwargs['partKey']))

        default_data = {}

        if kwargs['partKey'] == "Compaction":
            default_data = kwargs

        elif kwargs['partKey'] == "ODP-Daily" or kwargs['partKey'] == "SR-Entity" or kwargs[
            'partKey'] == "DQ-Framework" or kwargs['partKey'] == "ECOM-Daily" or kwargs['partKey'] == "CI-Summary" or \
                kwargs['partKey'] == "CI-CDS" or kwargs['partKey'] == "IBMF-Entity" or kwargs['partKey'] == "NPS-Spotfire":
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
            Subject='ODP Dynamodb Reset ' + task_status + ' - ' + task_date
        )
        return True

    except Exception as e:
        print("Not able to send notification!. Error {}".format(e))
        return False


def reset_flag(**ddb_sec_resp):
    temp_dict = {}

    for k, v in ddb_sec_resp.items():
        temp_dict.update({k: v})
        if v == "N":
            temp_dict.update({k: "Y"})

    return temp_dict


class customFail(Exception):
    pass


def main(sns_arn):
    try:

        # Check whether the entry present in Dynamodb
        first_partKey, first_sortKey = 'ODP-Daily', 'Compaction'
        ddb_first_response = ddb_get_unique_record(first_partKey, first_sortKey)
        RESET_FLAG_DATE = ddb_first_response["CURRENT_EXE_DATE"]
        print("Last Execution Date : {}".format(RESET_FLAG_DATE))

        try:
            second_partKey, second_sortKey = "Compaction", RESET_FLAG_DATE
            sr_entity_partKey, sr_entity_sortKey = "SR-Entity", RESET_FLAG_DATE
            dqf_partKey, dqf_sortKey = "DQ-Framework", RESET_FLAG_DATE
            ci_summ_pk, ci_summ_sk = "CI-Summary", RESET_FLAG_DATE
            ecom_pk, ecom_sk = "ECOM-Daily", RESET_FLAG_DATE
            ci_cds_pk, ci_cds_sk = "CI-CDS", RESET_FLAG_DATE
            ibmf_pk, ibmf_sk = "IBMF-Entity", RESET_FLAG_DATE
            nps_sp_pk, nps_sp_sk = "NPS-Spotfire", RESET_FLAG_DATE

            ddb_second_response = ddb_get_unique_record(second_partKey, second_sortKey)
            ddb_sr_entity_response = ddb_get_unique_record(sr_entity_partKey, sr_entity_sortKey)
            ddb_dqf_response = ddb_get_unique_record(dqf_partKey, dqf_sortKey)
            ddb_ci_summ_response = ddb_get_unique_record(ci_summ_pk, ci_summ_sk)
            ddb_ecom_response = ddb_get_unique_record(ecom_pk, ecom_sk)
            ddb_ci_cds_response = ddb_get_unique_record(ci_cds_pk, ci_cds_sk)
            ddb_ibmf_response = ddb_get_unique_record(ibmf_pk, ibmf_sk)
            ddb_nps_sp_response = ddb_get_unique_record(nps_sp_pk, nps_sp_sk)

            if ddb_second_response:

                print("Compaction Response {}".format(ddb_second_response))
                ddb_second_response_reset = reset_flag(**ddb_second_response)
                print("All flag reset to Y, {}".format(ddb_second_response_reset))
                ddb_second_response_reset['sortKey'] = date_yesterday
                ddb_second_response_reset['START_TIME'] = str(datetime.now())
                ddb_second_response_reset['UP_TIME'] = str(datetime.now())

                print("Compaction entry: {}".format(ddb_second_response_reset))
                print("Compaction Execution Date : {}".format(ddb_first_response))

                ddb_compaction_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "Compaction",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                ddb_update_records(**ddb_second_response_reset)
                ddb_update_records(**ddb_compaction_daily_dt_update)

                print("-*" * 80)
                ddb_sr_entity_response_reset = reset_flag(**ddb_sr_entity_response)
                ddb_sr_entity_response_reset['sortKey'] = date_yesterday

                ddb_sr_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "SR-Entity",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("SR sortKey update {}".format(ddb_sr_entity_response_reset))

                ddb_update_records(**ddb_sr_entity_response_reset)
                ddb_update_records(**ddb_sr_daily_dt_update)

                print("-*" * 80)
                ddb_dqf_response_reset = reset_flag(**ddb_dqf_response)
                ddb_dqf_response_reset['sortKey'] = date_yesterday

                ddb_dqf_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "DQ-Framework",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("DQF sortKey update {}".format(ddb_dqf_response_reset))

                ddb_update_records(**ddb_dqf_response_reset)
                ddb_update_records(**ddb_dqf_daily_dt_update)

                print("-*" * 80)
                ddb_ci_summ_response_reset = reset_flag(**ddb_ci_summ_response)
                ddb_ci_summ_response_reset['sortKey'] = date_yesterday

                ddb_ci_summ_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "CI-Summary",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("CI sortKey update {}".format(ddb_ci_summ_response_reset))

                ddb_update_records(**ddb_ci_summ_response_reset)
                ddb_update_records(**ddb_ci_summ_daily_dt_update)

                print("-*" * 80)
                ddb_ecom_response_reset = reset_flag(**ddb_ecom_response)
                ddb_ecom_response_reset['sortKey'] = date_yesterday

                ddb_ecom_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "ECOM-Daily",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("E-Comm sortKey update {}".format(ddb_ecom_response_reset))

                ddb_update_records(**ddb_ecom_response_reset)
                ddb_update_records(**ddb_ecom_daily_dt_update)

                print("-*" * 80)
                ddb_ci_cds_response_reset = reset_flag(**ddb_ci_cds_response)
                ddb_ci_cds_response_reset['sortKey'] = date_yesterday

                ddb_ci_cds_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "CI-CDS",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("CI Entity sortKey update {}".format(ddb_ci_cds_response_reset))

                ddb_update_records(**ddb_ci_cds_response_reset)
                ddb_update_records(**ddb_ci_cds_daily_dt_update)

                print("-*" * 80)
                ddb_ibmf_response_reset = reset_flag(**ddb_ibmf_response)
                ddb_ibmf_response_reset['sortKey'] = date_yesterday

                ddb_ibmf_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "IBMF-Entity",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("CI Entity sortKey update {}".format(ddb_ibmf_response_reset))

                ddb_update_records(**ddb_ibmf_response_reset)
                ddb_update_records(**ddb_ibmf_daily_dt_update)
                
                print("-*" * 80)
                ddb_nps_sp_response_reset = reset_flag(**ddb_nps_sp_response)
                ddb_nps_sp_response_reset['sortKey'] = date_yesterday

                ddb_nps_sp_daily_dt_update = {
                    "partKey": "ODP-Daily",
                    "sortKey": "NPS-Spotfire",
                    "CURRENT_EXE_DATE": date_yesterday
                }

                print("NPS Spotfire sortKey update {}".format(ddb_nps_sp_response_reset))

                ddb_update_records(**ddb_nps_sp_response_reset)
                ddb_update_records(**ddb_nps_sp_daily_dt_update)


                msg = "Hi All, the dynamodb entry for " + str(date_yesterday) + " is RESET"
                sns_notification(sns_arn, msg, "Alert", date_today)

            else:
                print("False... Inserting default values")
                raise customFail("default value not found in DDB")

        except Exception as e:
            print("Error while getting second ddb response {}".format(e))
            raise customFail(str(e))

    except Exception as e:
        print("Error in main {}".format(e))
        raise customFail(str(e))


def lambda_handler(event, context):
    try:
        current_account_number = context.invoked_function_arn.split(":")[4]
        sns_arn = 'arn:aws:sns:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':ODP_SFN_SUCCESS'
        main(sns_arn)
        return {"Status": "Success"}

    except KeyError as ke:
        print(" {} No such key!".format(ke))
        return {"Status": "Failed with KeyError"}

    except Exception as e:
        print("Error at lambda_handler {}".format(e))
        return {"Status": "Failed"}
