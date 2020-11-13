import boto3
import json
import pandas
import threading
import time
import sys
from datetime import datetime


def get_dynamodb_keys(table_name):
    dynamo_client = boto3.client('dynamodb',region_name=config['region_name'])
    response_table = dynamo_client.describe_table(
        TableName=table_name
    )
    table_key = []
    key_info = response_table['Table']['KeySchema']
    for key in key_info:
        table_key.append(key['AttributeName'])
    return table_key

def put_dynamodb_item(table_name,dynamo_data):
    dynamo_client = boto3.client('dynamodb',region_name=config['region_name'])
    curr_time = datetime.now()
    time_obj = curr_time.strftime("%d-%b-%Y %H:%M")
    update_date_dict = {}
    update_date_dict['S'] = time_obj
    last_update_by_dict = {}
    last_update_by_dict['S'] = '502818085'
    dynamo_data['cicd_last_updated_ts'] = update_date_dict
    dynamo_data['cicd_last_updated_by'] = last_update_by_dict
    response = dynamo_client.put_item(
        TableName=table_name,
        Item=dynamo_data
    )
    print('Modified table : ',table_name,'. Performed put_item() operation')
    
def get_dynamodb_item(table_name,dynamo_data,keys_data):
    dynamo_client = boto3.client('dynamodb',region_name=config['region_name'])
    if len(keys_data)>1:
        response = dynamo_client.get_item(
            TableName=table_name,
            Key={keys_data[0]:dynamo_data[keys_data[0]],keys_data[1]:dynamo_data[keys_data[1]]}
        )
    else:
        response = dynamo_client.get_item(
            TableName=table_name,
            Key={keys_data[0]:dynamo_data[keys_data[0]]}
        )
    if 'Item' in response:
        get_item_data = response['Item']
        #different_items = {k: dynamo_data[k] for k in dynamo_data if k in get_item_data and dynamo_data[k] != get_item_data[k]}
        different_items = False
        for key1 in dynamo_data.keys():
            if key1 in get_item_data.keys():
                if dynamo_data[key1]!=get_item_data[key1]:
                    different_items = True
                    break
            else:
                different_items = True
                break
        if different_items==True:
            print('Config file is different from existing data in Table : ',table_name,':',dynamo_data)
            put_dynamodb_item(table_name,dynamo_data)
        else:
            print('Config file matches with existing data in Table : ',table_name,':',dynamo_data)
    else:
        print('Config file contains new data row for Table : ',table_name,':',dynamo_data)
        put_dynamodb_item(table_name,dynamo_data)

if __name__ == '__main__':
    with open('../config1.json','r') as f:
        data = json.load(f)
        config = data['DynamoDBPROD']
    df = pandas.read_csv(config['job_name_list'])
    df = df.dropna(axis = 0, how = 'any')
    size = len(df)
    path_prefix = sys.argv[1]
    for index in range(size):
        filename = df['FileName'][index]
        track = df['Track'][index]
        pr_id = df['PR_ID'][index]
        print('For File : ',filename)
        check_parts = filename.split('_')
        if len(check_parts)>1:
            table_name = check_parts[0]
        else:
            table_name = filename
        file_path = path_prefix+config['json_path_prefix']+track+config['json_comp_path']+filename+'.json'
        with open(file_path,'r') as f:
            dynamo_data = json.load(f)
        table_keys = get_dynamodb_keys(table_name)
        for single_table_entry in dynamo_data:
            for data_key in single_table_entry:
                db_entry = single_table_entry[data_key]
                get_dynamodb_item(table_name,db_entry,table_keys)
