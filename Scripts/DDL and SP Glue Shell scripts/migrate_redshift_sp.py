import boto3
import json
import pandas
import threading
from pgdb import connect
from datetime import datetime
from awsglue.utils import getResolvedOptions

"""
If a copy command in a thread fails it will be logged(To be disscussed).

"""
# ARGS = getResolvedOptions(sys.argv,
#                                    ["INPUT_JSON_PATH","COMPACTION_LAYER_PATH",
#                                     "IAM_ROLE","HOST","DATABASE_NAME"])
#
# INPUT_JSON_PATH = ARGS['INPUT_JSON_PATH']
# COMPACTION_LAYER_PATH = ARGS['COMPACTION_LAYER_PATH']
# IAM_ROLE = ARGS['IAM_ROLE']
# HOST = ARGS['HOST']
# DATABASENAME = ARGS['DATABASE_NAME']
#BATCH_NUMBER = ARGS['BATCH_NUMBER']

def get_manifest_file():
     s3 = boto3.resource('s3', region_name='us-east-1')
     location_obj = s3.Object("odp-us-prod-raw",
                                      "servicesuite/ODP/manifest/manifest_sp_locations.json")
     manifest_file = json.loads((location_obj.get()['Body'].read().decode('utf-8')))
     return manifest_file
	
def get_manifest_pre_file():
     s3 = boto3.resource('s3', region_name='us-east-1')
     location_obj = s3.Object("odp-us-prod-raw",
                                      "servicesuite/ODP/manifest/manifest_sp_order.json")
     manifest_file = json.loads((location_obj.get()['Body'].read().decode('utf-8')))
     return manifest_file

def get_redshift_credentials():
     """
     To get username and password from Redshift
     secret manager.
     :param :
     :return:secret
     """
     client = boto3. \
          client("secretsmanager", region_name="us-east-1")
     get_secret_value_response = client. \
          get_secret_value(SecretId="redshift_secrets")
     secret = get_secret_value_response['SecretString']
     secret = json.loads(secret)
     return secret




def execute_copy_instance(files,sp_name):
     try :
          con = connect( host='us-prod-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com', user=REDSHIFT_USERNAME, password=REDSHIFT_PASSWORD, database='gehc_data',port= '5439' )
     
          copy_command = files
     
          cur = con.cursor()
          startTime = datetime.now()
     
          cur.execute(copy_command)
         
          con.commit()
     
          cur.close()
          con.close()
          print("Stored Procedure : ",sp_name," executed")
     except Exception as e:
          print ('EXCEPTION',e)
          SKIP_LIST.append(sp_name)    
          
def get_s3_objects(file_loc):
     s3 = boto3.resource('s3', region_name='us-east-1')
     location_obj = s3.Object("odp-us-prod-raw",
                                      file_loc)
     s3_file = location_obj.get()['Body'].read().decode('utf-8','ignore')
     return s3_file

if __name__ == '__main__':
    global REDSHIFT_USERNAME, REDSHIFT_PASSWORD, FILES,SKIP_LIST
    #SECRET = get_redshift_credentials()
    REDSHIFT_USERNAME = '502818085'
    REDSHIFT_PASSWORD = 'gAcw@xWKz6ZR'
    SKIP_LIST = []
    manifest_file = get_manifest_file()
    manifest_file_pre = get_manifest_pre_file()
    FILES_normal = manifest_file['fileLocations']
    FILES_pre = manifest_file_pre['fileLocations']
    FILES_pre.extend(file for file in FILES_normal if file not in FILES_pre)
    for file in FILES_pre:
        print(file,' is getting executed')
        file_rep = 'servicesuite/'+file
        sp_name_schema = file_rep.split('/')[-1].split('.')[0]
        sp_name = sp_name_schema+'.'+file_rep.split('/')[-1].split('.')[1]
        s3_obj = get_s3_objects(file_rep)
        execute_copy_instance(s3_obj,sp_name)
    print("Errors : ",SKIP_LIST)
    if len(SKIP_LIST)==0:
        print('SUCCESS')
    else:
        print('FAILURE')
