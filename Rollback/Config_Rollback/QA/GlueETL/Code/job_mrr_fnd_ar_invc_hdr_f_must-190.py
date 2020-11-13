"""job_mrr_fnd_ar_invc_hdr_f_must: AR_INVC_HDR_F Table Migration SCD TYPE-1 OP_VAL = T"""

import sys
import timeit
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
import Framework.conf_md5 as md5
from pyspark.sql.functions import col
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import boto3
import json
import re
from botocore.exceptions import ClientError
from pyspark.sql.utils import AnalysisException

# Developer:Deepak ,Email:deepak.kumar2@ge.com
# Type:SCD_TYPE_1 OP_VAL = T
# Target_Table:AR_INVC_HDR_F
# SourceSystem:MUST
# SourceTables:BILLINGS1,TAB006UK
# Arguments are passed from the job parameters
# Description:
# This PySpark Code used to migrate Data from Source MirrorTable
# to Target RedShiftTable
# Update at target with audit columns added.
# And maintaining versioning for history data with respect to
# business requirements using current_flag_ind Column.


# CurrentTimeStamp
DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
JOB_START_TIME = timeit.default_timer()
TIMESTAMP = datetime.timestamp(datetime.now())
# CurrentTimeStamp

CURRENT_DTM = datetime.fromtimestamp(TIMESTAMP)
# @params: [TempDir, JOB_NAME]
ARGS = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'source_db',
                                     'source_table1', 'source_table2',
                                     'rs_stage_schema', 'cd_database',
                                     'rs_target_schema', 'rs_stage_table',
                                     'source_system', 'rs_target_table',
                                     'stage_cols', 'target_cols',
                                     'glue_conn', 'rs_db', 'bkt_name',
                                     'iam_role', 'jdbc_url'])

# Variable Declaration
SRC_DB = ARGS['source_db']  # db_mrr_must
SRC_TABLE_1 = ARGS['source_table1']  # BILLINGS1
SRC_TABLE_2 = ARGS['source_table2']  # TAB006UK
TARGET_DATABASE_NAME = ARGS['rs_target_schema']  # hdl_services_fnd_data
STAGE_DATABASE_NAME = ARGS['rs_stage_schema']  # hdl_services_etl_stage
CD_DATABASE_NAME = ARGS['cd_database']  # hdl_services_etl_stage
TARGET_TABLE = ARGS['rs_target_table']  # ar_invc_hdr_f
SOURCE_SYSTEM = ARGS['source_system']  # must
JSON_FILE_NAME = TARGET_TABLE + \
    "_" + SOURCE_SYSTEM  # ar_invc_hdr_f_must
STAGE_TABLE = ARGS['rs_stage_table']  # ar_invc_hdr_f_stage_must
CTLG_CONNECTION = ARGS['glue_conn']  # TestRedshift3
REDSHIFTDB = ARGS['rs_db']  # usinnovationredshift
S3_BUCKET = ARGS['bkt_name']  # "odp-us-innovation-raw"
MD5_COLUMN_SCD1 = TARGET_TABLE + "_md5_scd1"  # ar_invc_hdr_f_md5_scd1
TARGET_TABLE_COLUMNS = ARGS['target_cols']  # As per DDL(col1,col2,col3)
STAGE_TABLE_COLUMNS = ARGS['stage_cols']  # As per DDL(col1,col2,col3)
DBTABLE_STG = STAGE_DATABASE_NAME + "." + STAGE_TABLE

URL = ARGS["jdbc_url"]
IAM_ROLE = ARGS["iam_role"]

SC = SparkContext()
GLUECONTEXT = GlueContext(SC)
SPARK = GLUECONTEXT.spark_session
JOB = Job(GLUECONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)
RUN_ID = ARGS['JOB_RUN_ID']
JOB_NAME = ARGS['JOB_NAME']
TEMPDIR = ARGS['TempDir']

SRC_NOTEMPTY = True
try:
    # @type: DataSource
    # @args: [database = "db_mrr_must",
    ## table_name = "billing"
    # transformation_ctx = "billing_df"]
    # @return: DynamicFrame
    # @inputs: []
    billing_df = GLUECONTEXT.create_dynamic_frame. from_catalog(
        database=SRC_DB, table_name=SRC_TABLE_1, transformation_ctx="billing_df") .toDF()

    # @type: DataSource
    # @args: [database = "db_mrr_must",
    ## table_name = "TAB006UK"
    # transformation_ctx = "curr_df"]
    # @return: DynamicFrame
    # @inputs: []
    curr_df = GLUECONTEXT.create_dynamic_frame. from_catalog(
        database=SRC_DB, table_name=SRC_TABLE_2, transformation_ctx="curr_df") .toDF()
except AnalysisException as ae:
    print('While Reading Data from Glue Catalog,I am in analysis exception')
    print('Printing the exception : {}'.format(ae))
    SRC_NOTEMPTY = False
except Exception as e:
    print('I am in all other exception')
    print('Printing all other exception: {}'.format(e))
    raise


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


def read_from_redshift(query, username, password):
    """
    To get dataframe from Redshift
    :param :query, username, password
    :return:redshift_df
    """
    redshift_df = SPARK.read \
        .format("com.databricks.spark.redshift") \
        .option("aws_iam_role", IAM_ROLE) \
        .option("url", URL) \
        .option("user", username) \
        .option("password", password) \
        .option("query", query) \
        .option("tempdir", TEMPDIR) \
        .load()
    return redshift_df


SECRET = get_redshift_credentials()
REDSHIFT_USERNAME = SECRET.get('username')
REDSHIFT_PASSWORD = SECRET.get('password')

if(SRC_NOTEMPTY):
    def get_filtered_source_data(billing_df, curr_df):
        """
        To get only proper Data and Required Fields that are used for
        Transformations Adding and Renaming the columns as per the
        Business Requirements.All GP function Logic is maintained here
        :param billing_df
        :param curr_df

        """

        billing_df.createOrReplaceTempView("BILLINGS1")
        curr_df.createOrReplaceTempView("TAB006UK")

        ar_invc_hdr_fs_df = SPARK.sql("""
        SELECT
        BILLINGS1.CORESX||'~'||BILLINGS1.NOFACT AS INVC_NBR,
        BILLINGS1.COSTT AS INVC_STAT,
        BILLINGS1.CTFACT AS INVC_TYP,
        BILLINGS1.DAEDFACT AS INVC_DT,
        BILLINGS1.NOORDCLI AS ORACLE_INVC_NBR,
        BILLINGS1.FK_JOBCORESX||'~'||BILLINGS1.FK_JOBNOAFF AS SRV_REQ_ID,
        'JOB' as SRV_REQ_TYP_KEY,
        BILLINGS1.CORESX||'~'||BILLINGS1.NOFACT as INVC_HDR_ID,
        BILLINGS1.CORESX||'~'||BILLINGS1.NOFACT||'~'||'BILLINGS1'||'~'||'MUST' as SRC_SYS_ID,
        'BILLINGS1' as INVC_HDR_TYP_KEY,
        'MUST' as source_name,
        'job_mrr_fnd_ar_invc_hdr_f_must' as 	posting_agent,
        'BILLINGS1' as 	data_origin,
        'C' as VERSN_FLG,
        BILLINGS1.NEGDEV as NEGOTN_CURR_CD,
        BILLINGS1.EDIDEV as ISNG_CURR_CD,
        BILLINGS1.TOCONV as EXCHNG_RT,
        TAB006UK.COEXT as CURR_CD,
        BILLINGS1.CORESX||'~'||BILLINGS1.NOCPTCLI as bill_to_cust_id,
        BILLINGS1.op_val as op_val,
        BILLINGS1.hvr_last_upd_tms as hvr_last_upd_tms
        FROM BILLINGS1 LEFT JOIN
        TAB006UK
        ON
        TRIM(TAB006UK.coint)=TRIM(BILLINGS1.CODEVS)
        """)

        prd_cust_d_query = """select SRC_SYS_ID,CUST_TYP_KEY,CUST_ID,source_name as SRC_NAM
        from {cd_database}.PRT_CUST_D
    	where source_name = 'MUST'
    	and CUST_TYP_KEY='BILL_TO'
    	and current_flag_ind='Y'
    	and end_dtm='9999-12-31 23:59:59'
    	""".format(cd_database=CD_DATABASE_NAME)

        prd_cust_d_df = read_from_redshift(prd_cust_d_query,
                                           REDSHIFT_USERNAME,
                                           REDSHIFT_PASSWORD)

        srv_sr_req_d_query = """select SRC_SYS_ID,SRV_REQ_TYP_KEY,
        SR_CLOS_DT,SRV_REQ_ID,source_name  as SRC_NAM
        from {target_database_name}.SRV_SR_REQ_D
    	where source_name = 'MUST'
    	and srv_req_typ_key='JOB'
    	""".format(target_database_name=TARGET_DATABASE_NAME)

        srv_sr_req_d_df = read_from_redshift(srv_sr_req_d_query,
                                             REDSHIFT_USERNAME,
                                             REDSHIFT_PASSWORD)

        ar_invc_hdr_fs_df_cond = [
            ar_invc_hdr_fs_df.SRV_REQ_ID == srv_sr_req_d_df.srv_req_id,
            ar_invc_hdr_fs_df.source_name == 'MUST',
            ar_invc_hdr_fs_df.source_name == srv_sr_req_d_df.src_nam]

        prd_cust_d_df_COND = [
            ar_invc_hdr_fs_df.bill_to_cust_id == prd_cust_d_df.cust_id,
            ar_invc_hdr_fs_df.source_name == prd_cust_d_df.src_nam,
            ar_invc_hdr_fs_df.source_name == 'MUST']

        filter_df1 = ar_invc_hdr_fs_df.alias('l')\
            .join(srv_sr_req_d_df.alias('r'),
                  ar_invc_hdr_fs_df_cond, 'left')\
            .select("l.*", "r.src_sys_id")\
            .withColumn('srv_req_sk', col('r.src_sys_id'))\
            .drop(col('r.src_sys_id'))

        filter_df2 = filter_df1.alias('l')\
            .join(prd_cust_d_df.alias('r'),
                  prd_cust_d_df_COND, 'left')\
            .select("l.*", 'r.src_sys_id', 'r.cust_typ_key')\
            .withColumn('bill_to_cust_src_sys_id', col('r.src_sys_id'))\
            .withColumn('bill_to_cust_typ', col('r.cust_typ_key'))\
            .drop(col('r.src_sys_id'))\
            .drop(col('r.cust_typ_key'))

        final_ranked_df = filter_df2 \
            .withColumn('rank', F.row_number()
                        .over(Window.partitionBy('INVC_HDR_ID')
                              .orderBy(F.desc('hvr_last_upd_tms')))) \
            .drop("hvr_last_upd_tms")
        return final_ranked_df

    def get_new_ins_upd_records(src_df):
        """
        Filtering New Inserted and updated records from Filtered source data
        :param FILTERED_SRC_DF:
        :return ins_upd_final_df:
        """
        ins_upd_df = src_df.filter(src_df.rank == 1)
        ins_upd_final_df = ins_upd_df \
            .withColumn('deleted_flag_ind', F.lit('N')) \
            .withColumn('current_flag_ind', F.lit('Y')) \
            .withColumn('end_dtm', F.lit('9999-12-31 23:59:59')
                        .astype('Timestamp')) \
            .withColumn('start_dtm', F.lit(DATETIMESTAMP)
                        .astype('Timestamp')) \
            .drop("rank")
        return ins_upd_final_df

    # Calling get_filtered_source_data Function
    STEP_LOG = "Step 1:get_filtered_source_data()-Method-Initiated \n"
    START_TIME = timeit.default_timer()
    try:
        FILTERED_SRC_DF = get_filtered_source_data(billing_df, curr_df)
        ELAPSED_TIME = timeit.default_timer() - START_TIME
        FILTERED_SRC_DF.persist()
        INPUT_RECORDS = FILTERED_SRC_DF.count()
        STEP_LOG = STEP_LOG + "Step 1:Succeeded \n" + "Step 1:Execution time: " + \
                                                      str(ELAPSED_TIME) + "\n"
        print(STEP_LOG)
    except Exception as exception:
        ELAPSED_TIME = timeit.default_timer() - START_TIME
        STEP_LOG = STEP_LOG + "Step 1:Failed with ERROR: " + \
                              str(exception) + "\nStep 1:Execution time: " + \
                                               str(ELAPSED_TIME) + "\n"
        print(STEP_LOG)

    # calling get_new_ins_upd_records function
    STEP_LOG = STEP_LOG + "Step 2:get_new_ins_upd_records()-Method-Initiated \n"
    START_TIME = timeit.default_timer()
    try:
        INSERT_UPDATE_FINAL_DF = get_new_ins_upd_records(FILTERED_SRC_DF)
        ELAPSED_TIME = timeit.default_timer() - START_TIME
        STEP_LOG = STEP_LOG + "Step 2:Succeeded \n" + "Step 2:Execution time: " + \
                                                      str(ELAPSED_TIME) + "\n"
        print(STEP_LOG)
    except Exception as exception:
        ELAPSED_TIME = timeit.default_timer() - START_TIME
        STEP_LOG = STEP_LOG + "Step 2:Failed with ERROR: " + \
                              str(exception) + "\nStep 2:Execution time: " + \
                                               str(ELAPSED_TIME) + "\n"
        print(STEP_LOG)

    # Final DF with md5 column generation for SCD_TYPE1 Columns
    # Calling getconfiguration_scd1() method from framework passed which takes
    # four arguments as input.
    # @ARGS:(S3_BUCKET,JSON_FILE_NAME,INSERT_UPDATE_FINAL_DF,MD5_COLUMN_SCD1)
    # Prerequisite:
    # A Json File(eg:<TARGET_TABLE>_<SOURCE_SYSTEM>.json) has to be created
    # in config_md5.zip which contains List of SCDType1 Columns and
    # List of Business Key and json file path has to be maintained in
    # 'manifest_file_locations.json'.Using this framework
    # FINAL_TUPLE_WITH_DF_AND_MD5 returns Tuple containing
    # DataFrame with md5 col and a list['Natural_Keys'].
    STEP_LOG = STEP_LOG + "Step 3:getconfiguration_scd1() Method from " + \
        "conf_md5 Framework" + \
        "-Initiated \n"
    START_TIME = timeit.default_timer()
    try:
        FINAL_TUPLE_WITH_DF_AND_MD5 = md5.getconfiguration_scd1(
            S3_BUCKET, JSON_FILE_NAME, INSERT_UPDATE_FINAL_DF, MD5_COLUMN_SCD1)
        ELAPSED_TIME = timeit.default_timer() - START_TIME
        FINAL_MD5_DF = FINAL_TUPLE_WITH_DF_AND_MD5[0]
        FILTERED_SRC_DF.unpersist()
        STEP_LOG = STEP_LOG + "Step 4:Succeeded \n" + "Step 5:Execution time: " + \
                                                      str(ELAPSED_TIME) + "\n"
        print(STEP_LOG)
    except Exception as exception:
        ELAPSED_TIME = timeit.default_timer() - START_TIME
        STEP_LOG = STEP_LOG + "Step 4:Failed with ERROR: " + \
                              str(exception) + "\nStep 5:Execution time: " + \
                                               str(ELAPSED_TIME) + "\n"
        print(STEP_LOG)

    # Returns a List['Natural_Key']
    NATURAL_KEY = FINAL_TUPLE_WITH_DF_AND_MD5[1]

    # Taking the natual key that passed in Json File.
    NATURAL_KEY_1 = NATURAL_KEY[0]

    # Taking the value from SOURCE_NAME column (example : "HR PERSON") from
    # FINAL_MD5_DF
    POST_QUERY_SOURCE_NAME = FINAL_MD5_DF.select(
        "source_name").limit(1).rdd.map(lambda a: a[0]).collect()[0]
    print('#######>>>>>>>POST_QUERY_SOURCE_NAME', POST_QUERY_SOURCE_NAME)

    # Final Data frame is converted to Dynamic frame
    # Final Dynamic Frame will be written to Stage Table
    FINAL_DYNAMIC_FRAME = DynamicFrame.fromDF(FINAL_MD5_DF,
                                              GLUECONTEXT,
                                              "Final_dynamic_frame")

    # Updates,Inserts and Deletes counts logic here
    # 1. Create a DF with counts and op_val, Group by JobId,op_val
    # 2. Extract inserts, updates and deletes
    # 3. Add it to Cloud Watch Logs.

    COUNT_DF = FINAL_MD5_DF.withColumn('JobRunId', F.lit(str(RUN_ID)))\
                           .withColumn('JobName', F.lit(str(RUN_ID)))

    # Truncating the stage table
    PRE_QUERY = """begin;
    truncate table {stage_database_name}.{stage_table};
    end;""".format(stage_database_name=STAGE_DATABASE_NAME,
                   stage_table=STAGE_TABLE)

    # Implementing the SCD1 logic between the stage and the target table.

    # Files with op_val = 'T'

    # Deletes : For op_val = 'T' , Soft delete the natural_key records present
    # in target_table and not present in current batch stage_table

    # No Change in stage_table record with opval = 'T', but present in target_table :
    # natural_key = natural_key & md5_column_scd1 = md5_column_scd1
    # Hard delete records from stage_table which matched the above condition

    # UPDATES(SCD Type1) :Update start_dtm in stage_table from target_table
    # based on current matched condition(to handle time box)
    ## Now, natural_key = natural_key & md5_column_scd1 != md5_column_scd1 ,
    # Soft Delete the matched records from target_table using Stage table.

    # Inserts :Once all are handled as above,We will insert all latest records
    # from stage table.

    POST_QUERY = """begin;
    Update {target_database_name}.{target_table}
    SET
    end_dtm = current_timestamp,
    deleted_flag_ind = 'Y',
    current_flag_ind='N'
    where {target_table}.{natural_key} in
    (
    select {natural_key} from {target_database_name}.{target_table} where {target_table}.source_name = '{post_query_source_name}'
    minus
    select {natural_key} from {stage_database_name}.{stage_table} where {stage_table}.source_name = '{post_query_source_name}'
    );

    delete from {stage_database_name}.{stage_table}
    using
    {target_database_name}.{target_table}
    where {stage_table}.{natural_key} = {target_table}.{natural_key}
    and {stage_table}.source_name = {target_table}.source_name
    and {stage_table}.{md5_column_scd1} = {target_table}.{md5_column_scd1}
    and {target_table}.current_flag_ind = 'Y'
    and {target_table}.end_dtm = '9999-12-31 23:59:59'
    and {stage_table}.op_val = 'T';

    update {stage_database_name}.{stage_table}
    set start_dtm = {target_table}.start_dtm
    from {target_database_name}.{target_table}
    where {stage_table}.{natural_key} = {target_table}.{natural_key}
    and {stage_table}.source_name = {target_table}.source_name
    and {stage_table}.{md5_column_scd1} != {target_table}.{md5_column_scd1}
    and {target_table}.current_flag_ind = 'Y'
    and {stage_table}.op_val = 'T';

    Update {target_database_name}.{target_table}
    SET
    end_dtm = current_timestamp,
    deleted_flag_ind = 'Y',
    current_flag_ind='N'
    from {stage_database_name}.{stage_table}
    where {stage_table}.{natural_key} = {target_table}.{natural_key}
    and {stage_table}.source_name = {target_table}.source_name
    and {stage_table}.{md5_column_scd1} != {target_table}.{md5_column_scd1}
    and {target_table}.current_flag_ind = 'Y'
    and {target_table}.end_dtm = '9999-12-31 23:59:59'
    and {stage_table}.op_val = 'T';

    insert into {target_database_name}.{target_table}
    ({target_table_columns})
    select {stage_table_columns} from {stage_database_name}.{stage_table};

    end;""".format(target_database_name=TARGET_DATABASE_NAME,
                   stage_database_name=STAGE_DATABASE_NAME,
                   stage_table=STAGE_TABLE,
                   target_table=TARGET_TABLE,
                   natural_key=NATURAL_KEY_1,
                   target_table_columns=TARGET_TABLE_COLUMNS,
                   stage_table_columns=STAGE_TABLE_COLUMNS,
                   post_query_source_name=POST_QUERY_SOURCE_NAME,
                   md5_column_scd1=MD5_COLUMN_SCD1)

    # DATASINK Load Start Time
    DATASINK_START_TIME = timeit.default_timer()

    # Wrting Data to Final Target Table by performing prequery and postquery
    DATA_SINK = GLUECONTEXT \
        .write_dynamic_frame \
        .from_jdbc_conf(frame=FINAL_DYNAMIC_FRAME,
                        catalog_connection=CTLG_CONNECTION,
                        connection_options={"preactions": PRE_QUERY,
                                            "dbtable": DBTABLE_STG,
                                            "database": REDSHIFTDB,
                                            "postactions": POST_QUERY},
                        redshift_tmp_dir=TEMPDIR,
                        transformation_ctx="DATA_SINK")

    # Job End Time
    JOB_END_TIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Job Elapsed Time
    JOB_ELAPSED_TIME = timeit.default_timer() - JOB_START_TIME

    # DATA SINK ELAPSED TIME TO TARGET REDSHIFT
    DATASINK_ELAPSED_TIME = timeit.default_timer() - DATASINK_START_TIME

    # Logging Below Details to Cloud Watch Logs
    COUNTS_DF2 = COUNT_DF.withColumn('StepLog', F.lit(str(STEP_LOG)))\
        .withColumn('JobName', F.lit(str(JOB_NAME)))\
        .withColumn('Job_start_time', F.lit(DATETIMESTAMP))\
        .withColumn('Job_end_time', F.lit(JOB_END_TIME))\
        .withColumn('Records_frm_src', F.lit(INPUT_RECORDS)) \
        .withColumn('Datasink_Elapsed_Time',
                    F.lit(DATASINK_ELAPSED_TIME)) \
        .withColumn('Job_Elapsed_Time', F.lit(JOB_ELAPSED_TIME))

    # Creating Temp View for COUNTS_DF2
    COUNTS_DF2.createOrReplaceTempView("final_counts_dataframe")

    # One DataLog Dataframe Written To Cloud Watch Logs
    # Flat Files dont have any op_val column so counts cant be calculated
    AUDITING_COUNTS_DF = SPARK.sql("""
      select
      JobName as JobName,
      JobRunId as JobId,
      Job_Start_Time,
      Job_End_Time,
      Job_Elapsed_Time,
      --coalesce(InsertsVal, 0) as InsertVal,
      --coalesce(UpdatesVal, 0) as UpdateVal,
      --coalesce(DeletesVal, 0) as DeleteVal,
      Datasink_Elapsed_Time,
      Records_frm_src,
      StepLog as StepLog
      from
      final_counts_dataframe
    """)

    # FINAL_COUNTS_DYNAMIC_FRAME = DynamicFrame.fromDF(AUDITINGCOUNTSDF,
    #                                                 glueContext,
    #                                                 "final_counts_dynamic_frame")
    # datasink5 = glueContext.write_dynamic_frame.\
    #    from_jdbc_conf(frame=FINAL_COUNTS_DYNAMIC_FRAME,
    #                   catalog_connection="TestMySQL",
    #                   connection_options={"dbtable": "ntiauditing",
    #                                       "database": "odpusinntidb"})

    # Logging all the above info To Cloud Watch Logs
    AUDITING_COUNTS_DF.show(10, False)

    JOB.commit()
