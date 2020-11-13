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
                       contingency_finished_flag,start_time_f,unsourced_snapshot_lock_date,unsourced_snapshot_lock_value):
    try:
        default_data = {
            "partKey": pk_value,
            "sortKey": sk_value,
            "daily_finished_flag": daily_finished_flag,
            "onhand_finished_flag": onhand_finished_flag,
            "order_finished_flag": order_finished_flag,
            "contingency_finished_flag": contingency_finished_flag,
            "mapp_compaction_runtime": start_time_f,
            "unsourced_snapshot_lock_date": unsourced_snapshot_lock_date,
            "unsourced_snapshot_lock": unsourced_snapshot_lock_value
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
# try:
    start_time = datetime.now()
    start_time_f = start_time.strftime("%Y%m%d%H%M")
    # print("Received event {}".format(event))
    DDB_KEY_TO_UPDATE = event["DDB_KEY_TO_UPDATE"]
    DDB_FLAG = event["DDB_FLAG"]
    
    
    
    # DDB_KEY_TO_UPDATE = "daily_finished_flag"
    print(DDB_KEY_TO_UPDATE)
    print(DDB_FLAG)
    if len(DDB_KEY_TO_UPDATE) != 0:

        partkey, sortkey = "MAPP_TRACK", DDB_SORT_KEY
        ddb_result = ddb_get_unique_record(partkey, sortkey)
        print(ddb_result)
        if ddb_result:
            daily_finished_flag = ddb_result["daily_finished_flag"]
            onhand_finished_flag = ddb_result["onhand_finished_flag"]
            order_finished_flag = ddb_result["order_finished_flag"]
            contingency_finished_flag = ddb_result["contingency_finished_flag"]
            unsourced_snapshot_lock_date = ddb_result["unsourced_snapshot_lock_date"]
            unsourced_snapshot_lock_value = ddb_result["unsourced_snapshot_lock"]

            if DDB_KEY_TO_UPDATE == "skip_contingency_finished_flag" and contingency_finished_flag == DDB_FLAG:
                 a = 1/0
                 return a
            else: 
                pass
            
            # if DDB_KEY_TO_UPDATE == "skip_daily_finished_flag" and daily_finished_flag == "Y":
            #      a = 1/0
            #      return a
            # else: 
            #     pass
            if DDB_KEY_TO_UPDATE == "daily_finished_flag":
                daily_finished_flag = DDB_FLAG
                ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag, contingency_finished_flag,start_time_f,unsourced_snapshot_lock_date,unsourced_snapshot_lock_value)
               
                return {"Success": "Success"}

           
            
            elif DDB_KEY_TO_UPDATE == "contingency_finished_flag":
                contingency_finished_flag = DDB_FLAG
                
                ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag,contingency_finished_flag,start_time_f,unsourced_snapshot_lock_date,unsourced_snapshot_lock_value)
                
                return {"Success": "Success"}
            
            
            
            elif DDB_KEY_TO_UPDATE == "onhand_finished_flag" :
                onhand_finished_flag = DDB_FLAG
                
                ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag,
                                   contingency_finished_flag,start_time_f,unsourced_snapshot_lock_date,unsourced_snapshot_lock_value)
                
                return {"Success": "Success"}
            
            
            
            elif DDB_KEY_TO_UPDATE == "order_finished_flag":
                order_finished_flag = DDB_FLAG
                
                ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag,
                                   contingency_finished_flag,start_time_f,unsourced_snapshot_lock_date,unsourced_snapshot_lock_value)
                
                return {"Success": "Success"}
            
            

# except KeyError:
#     raise KeyError

# except Exception as e:
#     print("Lambda error {}".format(e))
#     return {"Error": str(e)}

