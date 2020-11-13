import json
import boto3
import os

glue_client = boto3.client('glue')
s3_client = boto3.client('s3')


def get_one_tbl_detail(db_name, tbl_name):
    """
    Describe an existing table from crawler db.
    :param db_name: 
    :param tbl_name: 
    :return: 
    """
    try:
        response = glue_client.get_table(DatabaseName=db_name, Name=tbl_name)
        print('Table response {}'.format(response))
        return response

    except Exception as e:
        print('Error while read the table {}'.format(e))
        raise e


def check_dict_value(chk_val, b):
    """
    Compare and validate the Columns with json 
    :param chk_val: 
    :param b: 
    :return: 
    """
    for k, v in b.items():
        print("k {} v{}".format(k,v))
        if chk_val == k:
            return v


def change_col_names(table_response, json_cols):
    """
    Update column details
    :param table_response: 
    :param json_cols: 
    :return: 
    """
    tmp_dict = {}
    
    try:

        del table_response["Table"]["CreateTime"]
        del table_response["Table"]["UpdateTime"]
        del table_response["Table"]["LastAccessTime"]
    
    except KeyError:
        pass
    
    
    # tmp_dict.update({"PartitionKeys": table_response["Table"]["PartitionKeys"]})
    
    for k, v in table_response.items():
        tmp_dict.update({k: v})
        for table_cols in table_response['Table']['StorageDescriptor']['Columns']:
            for k1, v1 in table_cols.items():
                if check_dict_value(v1, json_cols):
                    table_cols.update({k1: check_dict_value(v1, json_cols)})
    
    
    tmp_dict["Table"]["PartitionKeys"] = table_response["Table"]["PartitionKeys"]
    
    del tmp_dict['ResponseMetadata']
    
    print("Modified Dict {}".format(tmp_dict))
    return tmp_dict


def update_table_schema(tmp_dict):
    """
    Update crawler table with new column names.
    :param tmp_dict: 
    :return: 
    """
    try:
        print("Updated ".format(tmp_dict))

        db_name = tmp_dict['Table']['DatabaseName']
        tbl_name = tmp_dict['Table']['Name']
        tbl_storage_desc = tmp_dict['Table']['StorageDescriptor']
        tbl_param = tmp_dict['Table']['Parameters']
        tbl_partitions = tmp_dict['Table']['PartitionKeys']
        
        tbl_input = {}
        tbl_input.update({'Name': tbl_name})
        tbl_input.update({'StorageDescriptor': tbl_storage_desc})
        tbl_input.update({'Parameters': tbl_param})
        tbl_input.update({'PartitionKeys': tbl_partitions})

        print('Table input {}'.format(tbl_input))
        update_table_response = glue_client.update_table(DatabaseName=db_name, TableInput=tbl_input)
        print("Table updated {}".format(update_table_response))
        
    except Exception as e:
        print('Error {}'.format(e))
        raise e


def s3_json_decoder(bucket_name, file_prefix):
    try:
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_prefix)
        f_data = json.loads(s3_response['Body'].read().decode("utf-8"))
        return f_data

    except Exception as e:
        print("Error {}".format(e))
        raise e


def lambda_handler(event, context):
    bucket_name = os.environ["BUCKET_NAME"]
    file_prefix = os.environ["FILE_PREFIX"]
    
    try:
        raw_json_data = s3_json_decoder(bucket_name, file_prefix)
        for i in raw_json_data:
            print("List {} {}".format(i, type(i)))
            for db_name in i.keys():
                for table in i[db_name].keys():
                    existing_table_response = get_one_tbl_detail(db_name, table)
                    changed_col_response = change_col_names(existing_table_response, i[db_name][table])
                    update_table_schema(changed_col_response)

        return {"Status": "Success"}
    
    except Exception as e:
        print("Lambda error {}".format(e))
        return {"Status": "Failed"}
