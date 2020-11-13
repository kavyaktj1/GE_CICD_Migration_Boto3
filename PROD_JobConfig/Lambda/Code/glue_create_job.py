import os
import json
import boto3
import time
from os.path import join

"""
Read a master glue job and create a new glue job with updated parameter values from manifest file.
"""

# Create object to access Glue and S3 service
glue_client = boto3.client("glue")
s3_client = boto3.client("s3")


# A function helps to update the parameter value in Glue job
def replace_json_values(new_job_name, glue_desc, s3_table_response, mjob, src_name, src_tbl):
    """
    Below are the required input to update the response

    :param new_job_name:
    :param glue_desc:
    :param s3_table_response:
    :param src_table_name:
    :return:
    """
    try:
        temp_dict = {}
        g_resp = glue_desc["Job"]["DefaultArguments"]

        for i in s3_table_response:

            job_names = [k for k in i.keys()]
            if mjob in job_names:

                jobs_srcs = [s for s in i[mjob].keys()]
                if src_name in jobs_srcs:
                    print("{} {} ".format(mjob, src_name))

                    for dak, dav in g_resp.items():
                        
                        temp_dict.update({dak: dav})

                        for k, v in i[mjob][src_name][src_tbl].items():
                            if dak == k:
                                temp_dict.update({dak: v})

        glue_desc["Job"]["Name"] = new_job_name
        glue_desc["Job"]["DefaultArguments"] = temp_dict

        return glue_desc

    except Exception as e:
        print("Change glue default args {}".format(e))
        return False


# A function to read manifest file content from S3 location

def read_s3_manifest_file(bucket_name, manifest_file):
    """
    For this function the bucket name
    :param bucket_name:
    :param manifest_file:
    :return:
    """
    try:
        read_s3_manifest_file = s3_client.get_object(Bucket=bucket_name, Key=manifest_file)
        read_s3_manifest_files = json.loads(read_s3_manifest_file['Body'].read().decode("utf-8"))

        return read_s3_manifest_files

    except Exception as e:
        print("Parameter exception {}".format(e))


def read_local_file(S3_Bucket_name, dest_s3_path):
    """
    This will write the list of glue jobs created from manifest file, upload from local to S3
    once all the jobs are created.

    :param S3_Bucket_name:
    :param dest_s3_path:
    :return:
    """
    temp_file_name = '/tmp/temp_file.json'

    file_name = dest_s3_path.split('/')[-1].replace('.json', '_output.json')
    tgt_file_name = '/'.join(dest_s3_path.split('/')[:-1]) + '/' + file_name

    # tgt_file_name = "temp/503177323/temp_file.json"

    # with open(temp_file_name, 'r') as f:
    #     f_content = json.loads(f.read())

    print("File content : {}".format(f_content))
    print("{} {} {}".format(temp_file_name, S3_Bucket_name, tgt_file_name))

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(temp_file_name, S3_Bucket_name, tgt_file_name)


def create_glue_job(glue_job_config):
    """
    Create a new glue job
    :param glue_job_config:
    :return:
    """
    try:
        print("Glue input conf {}".format(json.loads(glue_job_config)))

        create_config = json.loads(glue_job_config)
        
        del create_config["CreatedOn"]
        del create_config["LastModifiedOn"]
        del create_config["MaxCapacity"]
        
        # try:
        #     if len(create_config["WorkerType"]) != 0:
        #         del create_config["AllocatedCapacity"]
        
        # except KeyError:
        #     print("No worker type key while create ")
        #     pass

        create_glue_job_response = glue_client.create_job(**create_config)

        print("Glue create response {}".format(create_glue_job_response))

        print("Glue created ")
        return True
    
    except KeyError:
        pass

    except Exception as e:
        print("Glue job create error {}".format(e))

        return False


def describe_glue_job(glue_job_name):
    """
    Describe master glue job and get the job details.
    :param glue_job_name:
    :return:
    """
    try:
        desc_glue_job = glue_client.get_job(JobName=glue_job_name)
        print("Glue Job {}".format(desc_glue_job))
        return desc_glue_job

    except glue_client.exceptions.EntityNotFoundException:
        print("Glue job not found!")
        return False

    except Exception as e:
        print("Glue Job describe error {}".format(e))
        return False


def update_glue_job(glue_job_config):
    """
    If the glue job already exist, this function will only update the job parameters.
    :param glue_job_config:
    :return:
    """
    print("inside update {}".format(glue_job_config))
    try:

        mod_config = json.loads(glue_job_config)

        update_payload = {}
        update_payload["JobName"] = mod_config["Name"]

        del mod_config["Name"]
        del mod_config["CreatedOn"]
        del mod_config["LastModifiedOn"]
        del mod_config["MaxCapacity"]

        try:
            if len(mod_config["WorkerType"]) != 0:
                print("Worker Type {}".format(mod_config["WorkerType"]))
                
                del mod_config["AllocatedCapacity"]

        except KeyError:
            print("Worker type")
            pass
        
        update_payload["JobUpdate"] = mod_config

        print("updated payload {}".format(update_payload))
        update_glue_job_response = glue_client.update_job(**update_payload)

        print("Glue update response {}".format(update_glue_job_response))

        print("Glue updated ")
        return True

    except Exception as e:
        print("Glue job update error {}".format(e))
        
        return False


def lambda_handler(event, context):
    try:

        BUCKET_NAME = os.environ["BUCKET_NAME"]
        MANIFEST_FILE_NAME = os.environ["MANIFEST_NAME"]
        job_check_list = []
        

        s3_manifest_response = read_s3_manifest_file(BUCKET_NAME, MANIFEST_FILE_NAME)
        # Default master glue jobs list
        j_list = ["job_lnd_mrr_full_refresh", "job_lnd_mrr_incr_refresh_op_val",
                  "job_lnd_mrr_incr_refresh_without_op_val"]

        for jb in s3_manifest_response:
            raw_glue_job_name = [k for k in jb.keys()]
            
            with open('/tmp/temp_file.json', 'w') as tmp_file:
                
                for pfx in raw_glue_job_name:
                    # master job name
                    raw_glue_job_response = describe_glue_job(pfx)
    
                    src_name = [sk for sk in jb[pfx].keys()]
    
                    for src in src_name:
                        tbl_name = [tk for tk in jb[pfx][src].keys()]
    
                        for tbl in tbl_name:
    
                            
                            tmp_file_json = {}
    
                            if pfx == j_list[1]:
                                new_glue_job_name = "job_lnd_mrr_incr_refresh_ov_" + src + "_" + tbl
    
                            elif pfx == j_list[2]:
                                new_glue_job_name = "job_lnd_mrr_incr_refresh_no_ov_" + src + "_" + tbl
                            else:
                                new_glue_job_name = pfx + "_" + src + "_" + tbl
                            
                            print("New job name {}".format(new_glue_job_name))
                            job_check_list.append(new_glue_job_name)
    
                            if describe_glue_job(new_glue_job_name) is False:
                                modified_glue_job_response = replace_json_values(new_glue_job_name,
                                                                                 raw_glue_job_response,
                                                                                 s3_manifest_response, pfx, src, tbl)
    
                                # print("Glue Job {}".format(modified_glue_job_response))
    
                                tmp_file_json.update({new_glue_job_name: modified_glue_job_response["Job"]})
    
                                tmp_file_json[new_glue_job_name]["CreatedOn"] = str(
                                    tmp_file_json[new_glue_job_name]["CreatedOn"])
                                tmp_file_json[new_glue_job_name]["LastModifiedOn"] = str(
                                    tmp_file_json[new_glue_job_name]["LastModifiedOn"])
    
                                tmp_file.write(json.dumps(tmp_file_json, indent=4))
    
                                create_glue_response = create_glue_job(json.dumps(modified_glue_job_response["Job"]))
    
                                time.sleep(1)
                                if create_glue_response is True:
                                    print("Created glue job {}".format(new_glue_job_name))
                                    continue
                                else:
                                    print("Not able to create glue job {}".format(new_glue_job_name))
                                    continue
    
                            else:
    
                                modified_glue_job_response = replace_json_values(new_glue_job_name,
                                                                                 raw_glue_job_response,
                                                                                 s3_manifest_response, pfx, src, tbl)
    
                                print("{} modified response {}".format(new_glue_job_name, modified_glue_job_response))
                                tmp_file_json.update({new_glue_job_name: modified_glue_job_response["Job"]})
    
                                modified_glue_job_response["Job"]["CreatedOn"] = str(
                                    modified_glue_job_response["Job"]["CreatedOn"])
                                modified_glue_job_response["Job"]["LastModifiedOn"] = str(
                                    modified_glue_job_response["Job"]["LastModifiedOn"])
    
                                update_glue_response = update_glue_job(json.dumps(modified_glue_job_response["Job"]))
                                tmp_file.write(json.dumps(tmp_file_json, indent=4))
    
                                print("{} File exists update changes...".format(update_glue_response))
                                time.sleep(1)
                                if update_glue_response is True:
    
                                    print("Glue job updated {}".format(new_glue_job_name))
                                    continue
    
                                else:
                                    print("Not able to update glue job {}".format(new_glue_job_name))
                                    continue
                                
        print("Jobs check list {} Total {}".format(job_check_list, len(job_check_list)))

        # read_local_file(BUCKET_NAME, MANIFEST_FILE_NAME)

        return {"Success": "Success"}

    except Exception as e:
        print("Error : {}".format(e))
        return {"Error ": str(e)}
