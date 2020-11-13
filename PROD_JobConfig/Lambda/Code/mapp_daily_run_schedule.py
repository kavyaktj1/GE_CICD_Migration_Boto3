import json
from datetime import datetime, timedelta
import boto3
import os

'''
DDB Design
MAPP_TRACK(partKey), DEV(sortKey), daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag
'''

# Format the Dynamodb Table name
DDB_TABLE = os.environ["envprefix"] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

# Get the sort key from Environment variable.
DDB_SORT_KEY = os.environ["envprefix"].split("-")[1].upper()

# Create client
ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

sns_client = boto3.client('sns')

date_today = datetime.today().strftime("%d-%m-%Y")


def sns_notification(sns_arn, msg, task_status, task_date):
    try:
        response = sns_client.publish(
            TopicArn=sns_arn,
            Message=msg,
            Subject='MAPP Daily Scheduled Lambda ' + task_status + ' - ' + task_date
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


def lambda_handler(event, context):
    
       
    # DAILY_CONTIGENCY = event["DAILY_CONTIGENCY"]
        
    current_account_number = context.invoked_function_arn.split(":")[4]
    sns_arn = 'arn:aws:sns:' + os.environ['AWS_REGION'] + ':' + current_account_number + ':ODP_SF'

    partkey, sortkey = "MAPP_TRACK", DDB_SORT_KEY
    ddb_result = ddb_get_unique_record(partkey, sortkey)

    if ddb_result:    
        daily_finished_flag = ddb_result["daily_finished_flag"]
        onhand_finished_flag = ddb_result["onhand_finished_flag"]
        order_finished_flag = ddb_result["order_finished_flag"]
        contingency_finished_flag = ddb_result["contingency_finished_flag"]
        
        check = [daily_finished_flag,onhand_finished_flag,order_finished_flag] 

        if('N' in check):
            
            a = 1/0
            return a
        else:
            pass
        # else:
        #     if(DAILY_CONTIGENCY == "CONTIGENCY"):
    
        #         if(contingency_finished_flag == 'N'):
                    
        #             a = 1/0
        #             return a
        #         else:
        #             pass
        # if daily_finished_flag == "Y" and onhand_finished_flag == "Y" and order_finished_flag == "Y" and DAILY_CONTIGENCY == "DAILY":
            
        #     daily_finished_flag = "N"
        #     ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag)
        #     msg = "Hi Team,  The previous day execution completed successfully. Resetting Flag value to N for Daily Finished Flag"
        #     sns_notification(sns_arn, msg, "Success", date_today)
        #     return {"Success": "Success"}
        
        # if DAILY_CONTIGENCY == "CONTIGENCY" and daily_finished_flag == "N" and contingency_finished_flag == "Y":
            
        #     contingency_finished_flag = "N"
        #     ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag)
        #     msg = "Hi Team,  The previous day execution completed successfully. Resetting Flag value to N for Daily Finished Flag"
        #     sns_notification(sns_arn, msg, "Success", date_today)
        #     return {"Success": "Success"}
        

        # else:
        #     msg = "Hi Team, The previous day execution is not completed successfully. Can't proceed further exiting now."
        #     sns_notification(sns_arn, msg, "Failed", date_today)
        #     return {"Failed": "Failed"}



