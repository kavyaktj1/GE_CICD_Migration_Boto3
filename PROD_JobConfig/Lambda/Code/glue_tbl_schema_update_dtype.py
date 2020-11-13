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


def check_replace(glue_table_response, header_manifest_response):
    """

    :param glue_table_response:
    :param header_manifest_response:
    :return:
    """
    temp_list = []

    for gtr in glue_table_response:
        temp_dict = gtr
        for hmr in header_manifest_response:
            hmr_kys = [k for k in hmr.keys()]
            try:
                if gtr['Name'] == hmr_kys[0] and gtr["Type"] == hmr_kys[1]:
                    temp_dict.update({"Name": hmr[gtr["Name"]]})
                    temp_dict.update({"Type": hmr[gtr["Type"]]})

            except KeyError:
                continue

        temp_list.append(temp_dict)
        print("Updated columns {}".format(temp_dict))

    return temp_list


def change_col_names(table_response, json_cols_list):
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

        tmp_dict["Table"]["PartitionKeys"] = table_response["Table"]["PartitionKeys"]
    except KeyError:
        pass

    # tmp_dict.update({"PartitionKeys": table_response["Table"]["PartitionKeys"]})

    for k, v in table_response.items():
        tmp_dict.update({k: v})

    tmp_dict["Table"]["StorageDescriptor"]["Columns"] = json_cols_list

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

        tbl_input = {}
        tbl_input.update({'Name': tbl_name})
        tbl_input.update({'StorageDescriptor': tbl_storage_desc})
        tbl_input.update({'Parameters': tbl_param})
        try:
            tbl_partitions = tmp_dict['Table']['PartitionKeys']
            tbl_input.update({'PartitionKeys': tbl_partitions})
        except KeyError:
            pass

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
                    changed_col_response = check_replace(
                        existing_table_response["Table"]["StorageDescriptor"]["Columns"], i[db_name][table])
                    ch_col_names_response = change_col_names(existing_table_response, changed_col_response)
                    update_table_schema(ch_col_names_response)

        return {"Status": "Success"}

    except Exception as e:
        print("Lambda error {}".format(e))
        return {"Status": "Failed"}
