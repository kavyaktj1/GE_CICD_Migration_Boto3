"""SAM PRT_PERSN_D Table Migration SCD TYPE-2"""
import sys
from datetime import datetime
import json
import timeit
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql.functions import lit, col
from pyspark.context import SparkContext
import boto3
import Framework.conf_md5 as md5
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import broadcast
from pyspark.sql import types as T
# Developer:Sravan Kumar ,Email:sravan.kumar2@ge.com , SSO:503171857
# Type:SCD_TYPE_2
# Target_Table:PRT_PERSN_D
# SourceSystem:GLA
# SourceTables:prt_persn_d
# Python Dependency Files: config_md5.zip
# Config_md5 framework is passed as a dependency file for
# md5 Column generation for final dataframe.
# Arguments are passed from the job parameters
# Description:
# This PySpark Code used to migrate Data from Source MirrorTable
# to Target RedShiftTable With added deleted_flag_ind column,
# which is set to 'Y'(op_val=0) at target for deleted record
# at source(For maintaining soft delete at target).And
# (op_val=1) for New Inserted Records and For (op_val=2)
# Update records at target with audit columns(deleted_flag_ind,
# current_flag_ind,start_dtm,end_dtm) added.And maintaining
# versioning for history data for scd type 2 updates with respect
# to business requirements using current_flag_ind Column.


# CurrentTimeStamp
DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# Gives time in seconds for calculating elapsed time
JOB_START_TIME = timeit.default_timer()
# @params:
ARGS = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'source_db',
                                     'source_table', 'rs_target_schema',
                                     'rs_stage_schema', 'source_system',
                                     'rs_target_table', 'stage_cols',
                                     'target_cols', 'glue_conn',
                                     'rs_db', 'bkt_name', 'rs_stage_table'])

# Variable Declaration
SRC_DB = ARGS['source_db']  ##db_mrr_gla
SRC_TABLE_1 = ARGS['source_table']  ##prt_persn_d
TARGET_DATABASE_NAME = ARGS['rs_target_schema']  ##datafabric_cfd_dea
STAGE_DATABASE_NAME = ARGS['rs_stage_schema']  ##datafabric_cfd_dea
TARGET_TABLE = ARGS['rs_target_table']  ##prt_persn_d
SOURCE_SYSTEM = ARGS['source_system']  ##sfdc
JSON_FILE_NAME = TARGET_TABLE + \
                 "_" + SOURCE_SYSTEM ##prt_persn_d_sfdc
STAGE_TABLE = ARGS['rs_stage_table'] ##prt_persn_d_sfdc_stage
CTLG_CONNECTION = ARGS['glue_conn']  ##TestRedshift1
REDSHIFTDB = ARGS['rs_db']  ##usinnovationredshift
S3_BUCKET = ARGS['bkt_name']  ##"odp-us-innovation-raw"
MD5_COLUMN = TARGET_TABLE + "_md5"  ##prt_persn_d_md5
TARGET_TABLE_COLUMNS = ARGS['target_cols'] ##As per DDL(col1,col2,col3)
STAGE_TABLE_COLUMNS = ARGS['stage_cols']   ##As per DDL(col1,col2,col3)
DBTABLE_STG = STAGE_DATABASE_NAME + "." + STAGE_TABLE
MD5_COLUMN_SCD1 = TARGET_TABLE + "_md5_scd1"


SC = SparkContext()
GLUECONTEXT = GlueContext(SC)
SPARK = GLUECONTEXT.spark_session
JOB = Job(GLUECONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)
RUN_ID = ARGS['JOB_RUN_ID']
JOB_NAME = ARGS['JOB_NAME']
TEMPDIR = ARGS['TempDir']

DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
END_DTM = '9999-12-31 23:59:59'
JOB_START_TIME = timeit.default_timer()

# @type: DataSource
# @args: [database = "db_mrr_siebel_americas",
# table_name = "s_contact",
# transformation_ctx = "sam_s_contact_dyf"]
# @return: DynamicFrame
# @inputs: []
# creating DataFrames from source glue crawlers
PRT_PERSN_D_SOURCE = GLUECONTEXT.create_dynamic_frame. \
    from_catalog(database=SRC_DB, table_name=SRC_TABLE_1,
                 transformation_ctx="PRT_PERSN_D_SOURCE")

PRT_PERSN_D_SOURCE_DF = PRT_PERSN_D_SOURCE.toDF()

def get_filtered_source_data(s_df1):
    """
    Implementing business logic as per existing green plum code
    :param s_df1: HCBI_ES_HIER_OPH_SOURCE_DF
    :param s_df2: HCBI_ES_PROP_OPH_SOURCE_DF
    :param s_df3: EOPL_EOSL_SOURCE_DF
    :return:source_prd_main_hier_d_join
    """
    # Implementing business logic as per existing green plum code
    
    s_df1.createOrReplaceTempView("gla")

    source_prt_persn_d_df1 = SPARK.sql(""" SELECT
    prt_persn_idn as persn_idn,
    prt_persn_typ_idn as persn_typ_key,
    persn_id,
    fst_nam as first_nam,
    lst_nam as last_nam,
    full_nam,
    email_addr as email_id,
    cost_centr,
    persn_key,
    'prt_persn_d' as data_origin,
    'job_mrr_fnd_prt_persn_d_sfdc' as posting_agent,
    current_timestamp as load_dtm,
    current_timestamp as update_dtm,
    cast(src_cretn_ts as timestamp) as source_creation_dtm,
    cast(src_upd_ts as timestamp) as source_update_dtm,
    'SFDC' AS source_name,
    'C' as versn_flg,
    op_val,
    hvr_last_upd_tms
    from gla where src_idn = 236
    """)
    source_prt_persn_d_df1 = source_prt_persn_d_df1.withColumn("pohne_nbr", F.lit('')) \
        .withColumn("fe_nam", F.lit('')) \
        .withColumn("fe_id", F.lit('')) \
        .withColumn("fe_idn", F.lit('').astype(DecimalType())) \
        .withColumn("role", F.lit('')) \
        .withColumn("hr_sso", F.lit('')) \
        .withColumn("hr_nam", F.lit('')) \
        .withColumn("mgr_sso", F.lit('')) \
        .withColumn("mgr_nam", F.lit('')) 

    source_prt_persn_d_df1 = source_prt_persn_d_df1\
    .withColumn('rank',
                    F.row_number()
                    .over(Window.partitionBy('persn_id')
                          .orderBy(F.desc('source_update_dtm')))).filter(F.col("rank") == 1)

    return source_prt_persn_d_df1


def get_new_ins_upd_records(src_df):
    """
    Filtering New Inserted and updated records from Filtered source data
    :param src_df:
    :return ins_upd_final_df:
    """
    ins_upd_final_df = src_df \
        .withColumn('deleted_flag_ind', F.lit('N')) \
        .withColumn('current_flag_ind', F.lit('Y')) \
        .withColumn('end_dtm', F.lit('9999-12-31 23:59:59')
                    .astype('Timestamp'))\
        .withColumn('start_dtm', F.lit(DATETIMESTAMP)
                    .astype('Timestamp')) \
        .drop("rank")
    print("ins_upd_final_df", ins_upd_final_df.count())
    return ins_upd_final_df


# Calling get_filtered_source_data Function
STEP_LOG = "Step 1:get_filtered_source_data()-Method-Initiated \n"
START_TIME = timeit.default_timer()
try:
    FILTERED_SRC_DF = get_filtered_source_data(PRT_PERSN_D_SOURCE_DF)
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

# calling get_new_ins_upd_records function for op_val=1,2
STEP_LOG = STEP_LOG + "Step 2:get_new_ins_upd_records()-Method-Initiated \n"
START_TIME = timeit.default_timer()
try:
    INSERT_FINAL_DF = get_new_ins_upd_records(FILTERED_SRC_DF)
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



# Final DF with md5 column generation for SCD_TYPE2 Columns
# Calling getconfiguration() method from framework passed which takes
# four arguments as input.
# @ARGS:(S3_BUCKET,JSON_FILE_NAME,FINAL_UNION_DATAFRAME,MD5_COLUMN)
# Prerequisite:
# A Json File(eg:<TARGET_TABLE>_<SOURCE_SYSTEM>.json) has to be created
# in config_md5.zip which contains List of SCDType2 Columns and
# List of Business Key and json file path has to be maintained in
# 'manifest_file_locations.json'.Using this framework
# FINAL_TUPLE_WITH_DF_AND_MD5 returns Tuple containing
# DataFrame with md5 col and a list['Natural_Keys'].
STEP_LOG = STEP_LOG + "Step 3:getconfiguration() Method from " + \
           "configmd5 Framework" + \
           "-Initiated \n"
START_TIME = timeit.default_timer()
try:
    FINAL_TUPLE_WITH_DF_AND_MD5 = md5.getconfiguration_op_val_t(S3_BUCKET,
                                                                JSON_FILE_NAME,
                                                                INSERT_FINAL_DF,
                                                                MD5_COLUMN,
                                                                MD5_COLUMN_SCD1)
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    FINAL_MD5_DF = FINAL_TUPLE_WITH_DF_AND_MD5[0]
    FILTERED_SRC_DF.unpersist()
    STEP_LOG = STEP_LOG + "Step 3:Succeeded \n" + "Step 5:Execution time: " + \
                                                  str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)
except Exception as exception:
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    STEP_LOG = STEP_LOG + "Step 3:Failed with ERROR: " + \
                          str(exception) + "\nStep 3:Execution time: " + \
                                           str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)



## Returns a List['Natural_Key']
NATURAL_KEY = FINAL_TUPLE_WITH_DF_AND_MD5[1]

## Taking the natual key that passed in Json File.
NATURAL_KEY_1 = NATURAL_KEY[0]

##Taking the value from SOURCE_NAME column (example : "HR PERSON") from FINAL_MD5_DF
POST_QUERY_SOURCE_NAME = FINAL_MD5_DF.select("source_name").limit(1).rdd.map(lambda a: a[0]).collect()[0]
print('#######>>>>>>>POST_QUERY_SOURCE_NAME', POST_QUERY_SOURCE_NAME)
print("finalmd5")

FINAL_MD5_DF1 = FINAL_MD5_DF.drop_duplicates()


# Final Data frame is converted to Dynamic frame
# Final Dynamic Frame will be written to Stage Table
FINAL_DYNAMIC_FRAME = DynamicFrame.fromDF(FINAL_MD5_DF1,
                                          GLUECONTEXT,
                                          "Final_dynamic_frame")


#Updates,Inserts and Deletes counts logic here
# 1. Create a DF with counts and op_val, Group by JobId,op_val
# 2. Extract inserts, updates and deletes
# 3. Add it to Cloud Watch Logs.

COUNT_DF = FINAL_MD5_DF.withColumn('JobRunId', F.lit(str(RUN_ID)))\
                       .withColumn('JobName', F.lit(str(RUN_ID)))



## Truncating the stage table
PRE_QUERY = """begin;
truncate table {stage_database_name}.{stage_table};
end;""".format(stage_database_name=STAGE_DATABASE_NAME,
               stage_table=STAGE_TABLE)

## Implementing the SCD2 logic between the stage and the target table.

## Files with op_val = 'T'

## Deletes : For op_val = 'T' , Soft delete the natural_key records present in target_table and not present in current batch stage_table

## No Change in stage_table record with opval = 'T', but present in target_table :
## natural_key = natural_key & md5_column = md5_column & md5_column_scd1 = md5_column_scd1
## Hard delete records from stage_table which matched the above condition

## UPDATES(SCD Type2) : natural_key = natural_key & md5_column != md5_column & md5_column_scd1 = md5_column_scd1 ,
## Make inactive of active record in target_table using Stage Table.

## UPDATES(SCD Type2) : natural_key = natural_key & md5_column != md5_column & md5_column_scd1 != md5_column_scd1 ,
## Make inactive of active record in target_table using Stage Table.

## UPDATES(SCD Type1) :Update start_dtm in stage_table from target_table
## based on current matched condition(to handle time box)
## Now, natural_key = natural_key & md5_column = md5_column & md5_column_scd1 != md5_column_scd1 ,
## Delete the matched records from target_table using Stage table.

## Inserts :Once all are handled as above,We will insert all latest records
## from stage table.

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
and {stage_table}.{md5_column} = {target_table}.{md5_column}
and {stage_table}.{md5_column_scd1} = {target_table}.{md5_column_scd1}
and {target_table}.current_flag_ind = 'Y'
and {target_table}.end_dtm = '9999-12-31 23:59:59'
and {stage_table}.op_val = 'T';

update {target_database_name}.{target_table}
set end_dtm = {stage_table}.start_dtm - interval '1 second',
current_flag_ind = 'N'            
from {stage_database_name}.{stage_table}
where {stage_table}.{natural_key} = {target_table}.{natural_key}
and {stage_table}.source_name = {target_table}.source_name 
and {stage_table}.{md5_column} != {target_table}.{md5_column} 
and {target_table}.current_flag_ind = 'Y'
and {stage_table}.op_val = 'T'; 

update {stage_database_name}.{stage_table}
set start_dtm = {target_table}.start_dtm           
from {target_database_name}.{target_table}
where {stage_table}.{natural_key} = {target_table}.{natural_key}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.{md5_column} = {target_table}.{md5_column}
and {stage_table}.{md5_column_scd1} != {target_table}.{md5_column_scd1} 
and {target_table}.current_flag_ind = 'Y'
and {stage_table}.op_val = 'T'; 


delete from {target_database_name}.{target_table} 
using
{stage_database_name}.{stage_table} 
where {stage_table}.{natural_key} = {target_table}.{natural_key}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.{md5_column} = {target_table}.{md5_column}
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
               md5_column=MD5_COLUMN,
               md5_column_scd1=MD5_COLUMN_SCD1)


#DATASINK Load Start Time
DATASINK_START_TIME = timeit.default_timer()

#Wrting Data to Final Target Table by performing prequery and postquery
DATA_SINK = GLUECONTEXT \
    .write_dynamic_frame \
    .from_jdbc_conf(frame=FINAL_DYNAMIC_FRAME,
                    catalog_connection=CTLG_CONNECTION,
                    connection_options={"preactions": PRE_QUERY,
                                        "dbtable":DBTABLE_STG,
                                        "database":REDSHIFTDB,
                                        "postactions":POST_QUERY},
                    redshift_tmp_dir=TEMPDIR,
                    transformation_ctx="DATA_SINK")

#Job End Time
JOB_END_TIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#Job Elapsed Time
JOB_ELAPSED_TIME = timeit.default_timer() - JOB_START_TIME

#DATA SINK ELAPSED TIME TO TARGET REDSHIFT
DATASINK_ELAPSED_TIME = timeit.default_timer() - DATASINK_START_TIME

#Logging Below Details to Cloud Watch Logs
COUNTS_DF2 = COUNT_DF.withColumn('StepLog', F.lit(str(STEP_LOG)))\
                       .withColumn('JobName', F.lit(str(JOB_NAME)))\
                       .withColumn('Job_start_time', F.lit(DATETIMESTAMP))\
                       .withColumn('Job_end_time', F.lit(JOB_END_TIME))\
                       .withColumn('Records_frm_src', F.lit(INPUT_RECORDS)) \
                       .withColumn('Datasink_Elapsed_Time',
                                   F.lit(DATASINK_ELAPSED_TIME)) \
                       .withColumn('Job_Elapsed_Time', F.lit(JOB_ELAPSED_TIME))

#Creating Temp View for COUNTS_DF2
COUNTS_DF2.createOrReplaceTempView("final_counts_dataframe")

#One DataLog Dataframe Written To Cloud Watch Logs
#Flat Files dont have any op_val column so counts cant be calculated
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


AUDITING_COUNTS_DF.show(10, False)

JOB.commit()
