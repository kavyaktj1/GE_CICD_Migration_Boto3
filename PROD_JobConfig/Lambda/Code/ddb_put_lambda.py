import json
import boto3
import os


ODP_ENV = os.environ['envprefix'].split("-")[1].upper()
DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]



ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)

# if ODP_ENV == 'DEV':
#     ODP_ENV = 1

# elif ODP_ENV == 'QA':
#     ODP_ENV = 2

# elif ODP_ENV == 'PROD':
#     ODP_ENV = 3


class InsertError(Exception):
    pass

class InputError(Exception):
    pass

def ddb_put_item(SRC_NAME, BATCH_NUMBER):
    
    
    try:
    
        format_key = {
            "partKey": SRC_NAME,
            "sortKey": ODP_ENV,
            "CURRENT_BATCH" : int(BATCH_NUMBER) + 1
        }
        
        print('format {}'.format(format_key))

        ddb_put_response = ddb_table.put_item(Item=format_key)

        print("Insert {}".format(ddb_put_response))
        return 'Success'

    except Exception as e:
        print("Put error {}".format(e))
        raise InsertError(e)


def lambda_handler(event, context):
    print("{} {}".format(event, type(event)))
    
    if isinstance(event, str):
        event = json.loads(event)
    
    try:
        
        BATCH_NUMBER = event["INPUT"]["BATCH_NUMBER"]
        SRC_NAME = event["INPUT"]["SRC_NAME"] + "_BATCH"
        
        
        result = ddb_put_item(SRC_NAME, BATCH_NUMBER)

        return result

    except Exception as e:

        print("please check input {}".format(e))
        raise InputError('Input error')
