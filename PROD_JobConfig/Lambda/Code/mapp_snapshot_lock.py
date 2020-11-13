import json
from datetime import datetime, timedelta
import os
import boto3
import time

DDB_TABLE = os.environ["envprefix"] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

# Get the sort key from Environment variable.
DDB_SORT_KEY = os.environ["envprefix"].split("-")[1].upper()

# Create client
ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

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
    
def ddb_update_records(pk_value, sk_value,daily_finished_flag, onhand_finished_flag, order_finished_flag,contingency_finished_flag,mapp_compaction_runtime, start_time_f,unsourced_snapshot_lock_value):
    try:
        default_data = {
            "partKey": pk_value,
            "sortKey": sk_value,
            "daily_finished_flag":daily_finished_flag,
            "onhand_finished_flag":onhand_finished_flag,
            "order_finished_flag":order_finished_flag,
            "contingency_finished_flag":contingency_finished_flag,
            "mapp_compaction_runtime":mapp_compaction_runtime,
            "unsourced_snapshot_lock_date": start_time_f,
            "unsourced_snapshot_lock": unsourced_snapshot_lock_value
        }


        print('format {}'.format(default_data))
        ddb_put_response = ddb_table.put_item(Item=default_data)
#         ddb_put_response = ddb_table.put_item(
# 		Item=default_data, 
# 		ConditionExpression='attribute_not_exists(unsourced_snapshot_lock_date)')    # <> start_time_f')
	
        
        print("Insert {}".format(ddb_put_response))

    except Exception as e:
        print("Put error {}".format(e))
        return False

def lambda_handler(event, context):
    start_time = datetime.now()
    start_time_f = start_time.strftime("%Y%m%d")
    DDB_KEY_TO_UPDATE = event["DDB_KEY_TO_UPDATE"] 
    print(start_time_f)
    if len(DDB_KEY_TO_UPDATE) != 0 and DDB_KEY_TO_UPDATE != "sleep":
        
        current_account_number = context.invoked_function_arn.split(":")[4]
        partkey, sortkey = "MAPP_TRACK", DDB_SORT_KEY
        ddb_result = ddb_get_unique_record(partkey, sortkey)
        
        if ddb_result:
            
            daily_finished_flag = ddb_result["daily_finished_flag"]
            onhand_finished_flag = ddb_result["onhand_finished_flag"]
            order_finished_flag = ddb_result["order_finished_flag"]
            contingency_finished_flag = ddb_result["contingency_finished_flag"]
            mapp_compaction_runtime = ddb_result["mapp_compaction_runtime"]
            unsourced_snapshot_lock_date = ddb_result["unsourced_snapshot_lock_date"]
            unsourced_snapshot_lock_value = ddb_result["unsourced_snapshot_lock"]
            
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            print(unsourced_snapshot_lock_date)
        
                
        if(DDB_KEY_TO_UPDATE == 'daily_d' and start_time_f != unsourced_snapshot_lock_date ):
            unsourced_snapshot_lock_value = "D"
            ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag,contingency_finished_flag,mapp_compaction_runtime, start_time_f,unsourced_snapshot_lock_value)
            time.sleep(30)
            return {"Success": "Success"}
            
        if(DDB_KEY_TO_UPDATE == 'contigency_c' and start_time_f != unsourced_snapshot_lock_date  ):
            unsourced_snapshot_lock_value = "C"
            ddb_update_records(partkey, sortkey, daily_finished_flag, onhand_finished_flag, order_finished_flag,contingency_finished_flag,mapp_compaction_runtime, start_time_f,unsourced_snapshot_lock_value)
            time.sleep(30)
            return {"Success": "Success"}
            
        if(DDB_KEY_TO_UPDATE == 'contigency_c_check' ):
            if(unsourced_snapshot_lock_value == "C"):
                pass
            else:
                a = 1/0
                return a
        if(DDB_KEY_TO_UPDATE == 'daily_d_check' ):
            if(unsourced_snapshot_lock_value == "D"):
                pass
            else:
                a = 1/0
                return a
    if len(DDB_KEY_TO_UPDATE) != 0 and DDB_KEY_TO_UPDATE == "sleep":        
        time.sleep(30)
        return {"Success": "Success"}