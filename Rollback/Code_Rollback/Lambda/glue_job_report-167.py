import json
import boto3
from datetime import datetime
import csv
import os

GLUE_CLIENT = boto3.client('glue1')
S3_CLINET = boto3.client('s3')

def reformat_list(list_val):
    r_list = []

    for i in list_val:
        for k, v in i.items():
            # print(v)
            r_list.append(v)
    return r_list


def generate_csv_report(history_resp_list, s3_bucket_name, s3_folder_path):

    s3_file_name = s3_folder_path + 'glue_job_report-' + str(datetime.now().strftime('%Y-%m-%d_%H-%M-%S')) + '.csv'
    tmp_file_name = '/tmp/glue_job_report.csv'
    # print('local {}, s3 {}'.format(tmp_file_name, s3_file_name))

    kys = history_resp_list[0].keys()

    with open(tmp_file_name, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, kys)
        dict_writer.writeheader()
        dict_writer.writerows(history_resp_list)

    # Upload file to s3 from temp folder.
    S3_CLINET.upload_file(tmp_file_name, s3_bucket_name, s3_file_name)


def convert_date_format(in_date):
    dt_fmt = in_date.strftime('%Y-%m-%d %H:%M:%S')
    return dt_fmt


def calc_time_difference(dt_one, dt_two):
    start_dt = datetime.strptime(str(dt_one), '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(str(dt_two), '%Y-%m-%d %H:%M:%S')

    time_difference = end_dt - start_dt
    time_difference_mins = (time_difference.total_seconds() / 60)

    return time_difference_mins


def get_job_history(glue_hist_dict):
    try:

        result_dict = {}
        resp_dict = {}

        for glue_job_name, run_ids in glue_hist_dict.items():
            for run_id in run_ids:

                # print('RUN ID {}'.format(run_ids))
                run_resp = GLUE_CLIENT.get_job_run(JobName=glue_job_name, RunId=run_id, PredecessorsIncluded=True)
                # print('run response {}'.format(run_resp))

                glue_jobs_name = run_resp['JobRun']['JobName']
                glue_job_start = convert_date_format(run_resp['JobRun']['StartedOn'])
                glue_job_stop = convert_date_format(run_resp['JobRun']['CompletedOn'])
                glue_job_status = run_resp['JobRun']['JobRunState']
                glue_job_exe_time = run_resp['JobRun']['ExecutionTime']
                glue_job_duration = calc_time_difference(glue_job_start, glue_job_stop)

                # resp_dict.update({'JOB_RUN_ID': run_id})
                resp_dict.update({'JOB_NAME': glue_jobs_name})
                resp_dict.update({'START_TIME': glue_job_start})
                resp_dict.update({'STOP_TIME': glue_job_stop})
                resp_dict.update({'JOB_STATUS': glue_job_status})
                resp_dict.update({'JOB_EXECUTION_TIME_IN_SEC': glue_job_exe_time})
                resp_dict.update({'START_STOP_DIFF_MINS': glue_job_duration})

                if glue_job_status == 'FAILED':
                    error_message = run_resp['JobRun']['ErrorMessage']
                    resp_dict.update({'ERROR_MESSAGE': error_message})

                # print('temp dict {}'.format(resp_dict))
                result_dict.update({run_id: resp_dict})

        # print('Result {}'.format(result_dict))
        return result_dict

    except Exception as history_error:
        print('Get history Error {}'.format(history_error))
        return None


def glue_jbk_history(glue_job_name):
    """
    A function to check whether the

    :param glue_job_name:
    :return:
    """

    try:
        run_id_list = []
        run_id_dict = {}

        glue_jbk_response = GLUE_CLIENT.get_job_bookmark(JobName=glue_job_name)
        jbk_run_id = glue_jbk_response['JobBookmarkEntry']['RunId']
        run_id_list.append(jbk_run_id)
        run_id_dict.update({glue_job_name: run_id_list})

        return run_id_dict

    except Exception as glue_error:
        print('glue error {}'.format(glue_error))
        return None


def lambda_handler(event, context):
    #glue_jobs = ["job_mrr_fnd_prd_main_rel_a_sbom", "job_mrr_fnd_prd_main_d_mws", "job_mrr_fnd_prd_main_rel_a_mws", "job_mrr_fnd_prd_main_hier_d_oph", "job_mrr_fnd_prd_main_d_oph", "job_mrr_fnd_geo_addr_d_sit", "job_mrr_fnd_prd_main_d_sit", "job_mrr_fnd_prt_cust_persn_a_sit", "job_mrr_fnd_prt_persn_d_sit", "job_mrr_fnd_geo_addr_d_sam", "job_mrr_fnd_prt_persn_d_sam", "job_mrr_fnd_prd_main_d_sam", "job_mrr_fnd_prt_cust_persn_a_sam", "job_mrr_fnd_prt_cust_d_sam", "job_mrr_fnd_geo_addr_d_must", "job_mrr_fnd_prd_main_d_must", "job_mrr_fnd_prt_persn_d_must", "job_mrr_fnd_prt_cust_d_must", "job_mrr_fnd_gcd_curr_main_d_glprod", "job_mrr_fnd_geo_main_d_glprod", "job_mrr_fnd_gl_set_of_boks_d_glprod", "job_mrr_fnd_inv_bom_cst_typ_d_glprod", "job_mrr_fnd_inv_gpo_routng_d_glprod", "job_mrr_fnd_inv_itm_loc_d_glprod", "job_mrr_fnd_inv_param_d_glprod", "job_mrr_fnd_inv_trans_typ_d_glprod", "job_mrr_fnd_org_main_d_glprod", "job_mrr_fnd_prd_main_d_glprod", "job_mrr_fnd_prd_main_item_price_d_glprod", "job_mrr_fnd_qp_list_headers_b_d_glprod", "job_mrr_fnd_qp_list_headers_tl_d_glp", "job_mrr_fnd_qp_list_lines_d_glprod", "job_mrr_fnd_qp_pricing_d_glprod", "job_mrr_fnd_qp_qualifiers_d_glprod", "job_mrr_fnd_svcops_op_mor_fx_load_glprod", "job_mrr_fnd_geo_addr_d_glprod", "job_mrr_fnd_prt_persn_d_glprod", "job_mrr_fnd_geo_addr_d_sfdc", "job_mrr_fnd_prt_cust_d_sfdc_gla", "job_mrr_fnd_prt_cust_typ_d_sfdc", "job_mrr_fnd_prt_cust_ucm_comrcl_hier_d_sfdc", "job_mrr_fnd_prt_cust_ucm_hier_d_sfdc", "job_mrr_fnd_prt_persn_d_sfdc", "job_mrr_fnd_prt_cust_dtl_d_sfdc", "job_mrr_fnd_prd_main_d_smax", "job_mrr_fnd_geo_addr_d_smax", "job_mrr_fnd_prt_persn_d_smax", "job_mrr_fnd_prt_cust_d_smax", "job_mrr_fnd_cc_country_hier_drm", "job_mrr_fnd_gl_es_cost_center_org_hier_drm", "job_mrr_fnd_gl_es_reg_hier_drm", "job_mrr_fnd_gl_es_segmnt2_d_drm", "job_mrr_fnd_gl_es_segmnt1_d_drm", "job_mrr_fnd_geo_addr_d_hr_flat_file", "job_mrr_fnd_prt_persn_d_hr", "job_mrr_fnd_geo_addr_d_caps_flat_file", "job_mrr_fnd_geo_main_d_caps", "job_mrr_fnd_prd_main_d_caps", "job_mrr_fnd_prt_persn_d_caps", "job_mrr_fnd_prt_cust_d_caps", "job_mrr_fnd_geo_addr_d_cares_flat_file", "job_mrr_fnd_prd_main_d_cares", "job_mrr_fnd_prt_cust_d_cares"]
    glue_jobs = ["job_mrr_fnd_prd_main_rel_a_sbom", "job_mrr_fnd_prd_main_d_mws", "job_mrr_fnd_prd_main_rel_a_mws", "job_mrr_fnd_prd_main_hier_d_oph", "job_mrr_fnd_prd_main_d_oph", "job_mrr_fnd_geo_addr_d_sit", "job_mrr_fnd_prd_main_d_sit", "job_mrr_fnd_prt_cust_persn_a_sit"]
    temp_list = []
    S3_BUCKET_NAME = os.environ['SRC_BUCKET_NAME']
    S3_FOLDER_PATH = 'servicesuite/logs/'

    for jb in glue_jobs:
        job_bk_history = glue_jbk_history(jb)

        if job_bk_history is not None:
            hist_list = get_job_history(job_bk_history)
            temp_list.append(hist_list)

    print('Final Result {}'.format(temp_list))
    r_list = reformat_list(temp_list)
    generate_csv_report(r_list, S3_BUCKET_NAME, S3_FOLDER_PATH)
