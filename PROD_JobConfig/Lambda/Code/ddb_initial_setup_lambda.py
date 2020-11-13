import json
import boto3
import os

ODP_ENV = os.environ['envprefix'].split("-")[1].upper()
DDB_TABLE = os.environ['envprefix'] + "-mirrorMetadata"
DDB_REGION = os.environ["region"]

ddb_client = boto3.resource("dynamodb", region_name=DDB_REGION)
ddb_table = ddb_client.Table(DDB_TABLE)


class InsertError(Exception):
    pass


class InputError(Exception):
    pass


def ddb_get_item(SRC_NAME):
    """
    This function will check whether the given source system
    already present in Database,
    1. If Current Batch number 0 or Part key not present then call the ddb_put_item()
    2. If any value exist then will skip without modify the current batch number.
    :param SRC_NAME:
    :return:
    """
    try:
        format_key = {
            "partKey": SRC_NAME,
            "sortKey": ODP_ENV
        }

        ddb_get_response = ddb_table.get_item(Key=format_key)

        return int(ddb_get_response['Item']['CURRENT_BATCH'])

    except KeyError:
        # This raised if any new source added in the list.
        return False
    except Exception as e:
        print("Error {}".format(e))
        return False


def ddb_put_item(SRC_NAME):
    """
    :param SRC_NAME:
    :return:
    """
    print("{} Newly created".format(SRC_NAME))
    try:
        format_key = {
            "partKey": SRC_NAME,
            "sortKey": ODP_ENV,
            "CURRENT_BATCH": 0
        }

        print('format {}'.format(format_key))
        ddb_put_response = ddb_table.put_item(Item=format_key)

        print("Insert {}".format(ddb_put_response))
        return True

    except Exception as e:
        print("Put error {}".format(e))
        raise InsertError(e)

#Main starts here...
def lambda_handler(event, context):

    # List all your source system names with _BATCH in suffix
    SRC_NAMES = ["BFE_BATCH", "CHU_BATCH", "CONNECTIVITY_BATCH",
                 "DRM_BATCH", "FLAT_FILE_BATCH", "GIB_BATCH", "GLA_BATCH", "GLPROD_BATCH",
                 "MUST_BATCH", "MWS_BATCH", "PARTS_FILES_BATCH", "PQW_BATCH", "SALESFORCE_BATCH",
                 "SIEBEL_AMERICAS_BATCH", "SBOM_BATCH", "SIT_BATCH", "SERVICEMAX_BATCH"]
    try:
        for SRC_NAME in SRC_NAMES:
            print("SRC Name : {}".format(SRC_NAME))
            if not ddb_get_item(SRC_NAME):
                print("{} Not present...".format(SRC_NAME))
                ddb_put_item(SRC_NAME)
        
        return {"Status": "Success"}
    
    except Exception as e:
        return {"Status": str(e)}
