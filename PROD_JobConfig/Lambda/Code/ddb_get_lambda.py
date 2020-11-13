import json
import boto3
import os

ODP_ENV = os.environ['envprefix'].split("-")[1].upper()
DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]



ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)


class InputError(Exception):
    pass

def ddb_get_item(SRC_NAME):

    try:

        format_key = {
            "partKey": SRC_NAME,
            "sortKey": ODP_ENV
        }

        ddb_get_response = ddb_table.get_item(Key=format_key)
        print("Get response {}".format(ddb_get_response))

        return int(ddb_get_response['Item']['CURRENT_BATCH'])

    except Exception as e:

        print("Error {}".format(e))
        return -1


def lambda_handler(event, context):

    try:
        
        SRC_NAME = event['INPUT']['SRC_NAME'] + "_BATCH"
        # print(SRC_NAME)
        # print("Region {} Table {} env {}".format(ODP_ENV, DDB_TABLE, DDB_REGION))
        result = ddb_get_item(SRC_NAME)
        return str(result)

    except Exception as e:

        print("please check input {}".format(e))
        raise InputError('Input error')
