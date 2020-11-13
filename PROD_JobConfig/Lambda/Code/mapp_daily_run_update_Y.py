import json
from datetime import datetime, timedelta
import boto3
import os

'''
DDB Design
MAPP_TRACK(partKey), DEV(sortKey), daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag
'''
        
DDB_TABLE = os.environ["envprefix"] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

# Get the sort key from Environment variable.
DDB_SORT_KEY = os.environ["envprefix"].split("-")[1].upper()

# Create client
ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

sns_client = boto3.client('sns')

date_today = datetime.today().strftime("%d-%m-%Y")

# Format the Dynamodb Table name

def sns_notification(sns_arn, msg, task_status, task_date):
    try:
        response = sns_client.publish(
            TopicArn=sns_arn,
            Message=msg,
            Subject='MAPP Daily Scheduled Update Lambda ' + task_status + ' - ' + task_date
        )
        return True

    except Exception as e:
        print("Not able to send notification!. Error {}".format(e))
        return False


def ddb_get_unique_record(partkey, sortkey):
    try:
        format_key = {
            "partKey": partkey,
            "sortKey": sortkey
        }

        ddb_get_response = ddb_table.get_item(Key=format_key)
        print("Get response {}".format(ddb_get_response))
        return ddb_get_response["Item"]

    except Exception as e:
        print("DDB Get record error {}".format(e))
        return False


def ddb_update_records(pk_value, sk_value, daily_finished_flag, onhand_finished_flag, order_finished_flag,
                       contingency_finished_flag):
    try:
        default_data = {
            "partKey": pk_value,
            "sortKey": sk_value,
            "daily_finished_flag": daily_finished_flag,
            "onhand_finished_flag": onhand_finished_flag,
            "order_finished_flag": order_finished_flag,
            "contingency_finished_flag": contingency_finished_flag
        }

        print('format {}'.format(default_data))
        ddb_put_response = ddb_table.put_item(Item=default_data)
        print("Insert {}".format(ddb_put_response))

    except Exception as e:
        print("Put error {}".format(e))
        return False

class flagUpdateError(Exception):
    pass

def lambda_handler(event, context):
    try:
        print("Received event {}".format(event))
        #DDB_KEY_TO_UPDATE = event["DDB_KEY_TO_UPDATE"]
        DDB_KEY_TO_UPDATE = "daily_finished_flag"
        print(DDB_KEY_TO_UPDATE)
        if len(DDB_KEY_TO_UPDATE) != 0:
            current_account_number = context.invoked_function_arn.split(":")[4]
            sns_arn = 'arn:aws:sns:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':ODP_SF'

            partkey, sortkey = "MAPP_TRACK", DDB_SORT_KEY
            ddb_result = ddb_get_unique_record(partkey, sortkey)
            print("test")
            print(ddb_result)
            print(ddb_table)
            if ddb_result:
                daily_finished_flag = ddb_result["daily_finished_flag"]
                onhand_finished_flag = ddb_result["onhand_finished_flag"]
                order_finished_flag = ddb_result["order_finished_flag"]
                contingency_finished_flag = ddb_result["contingency_finished_flag"]

                if DDB_KEY_TO_UPDATE == "contingency_finished_flag" and contingency_finished_flag == "Y":
                    ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag)
                    msg = "Hi,  The contingency_finished_flag is Y, starting SNAPSHOT..."
                    sns_notification(sns_arn, msg, "Success", date_today)
                    return {"Success": "Success"}
                
                elif DDB_KEY_TO_UPDATE == "contingency_finished_flag" and contingency_finished_flag == "N":
                    # contingency_finished_flag = "N"
                    # ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag)
                    msg = "Hi,  The contingency_finished_flag can't change still it is running..."
                    sns_notification(sns_arn, msg, "Success", date_today)
                    return {"Success": "Success"}

                elif DDB_KEY_TO_UPDATE == "daily_finished_flag" and daily_finished_flag == "N":
                    daily_finished_flag = "Y"
                    ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag,
                                       contingency_finished_flag)
                    msg = "Hi,  The daily_finished_flag is updated as N"
                    sns_notification(sns_arn, msg, "Success", date_today)
                    return {"Success": "Success"}

    except KeyError:
        raise KeyError

    except Exception as e:
        print("Lambda error {}".format(e))
        return {"Error": str(e)}
