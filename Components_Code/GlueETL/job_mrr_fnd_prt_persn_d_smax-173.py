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

# Developer:Sravan Kumar ,Email:sravan.kumar2@ge.com SSO:503171857
# Type:SCD_TYPE_2
# Target_Table:PRT_PERSN_D
# SourceSystem:db_mrr_siebel_americas
# SourceTables:s_contact,s_user,s_srv_req
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
#ENDDTM ='9999-12-31 23:59:59'
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
SRC_DB = ARGS['source_db']  ##db_mrr_siebel_americas
SRC_TABLE_1 = ARGS['source_table']  ##contact

TARGET_DATABASE_NAME = ARGS['rs_target_schema']  ##datafabric_cfd_dea
STAGE_DATABASE_NAME = ARGS['rs_stage_schema']  ##datafabric_cfd_dea
TARGET_TABLE = ARGS['rs_target_table']  ##prt_persn_d
SOURCE_SYSTEM = ARGS['source_system']  ##SVMXC
JSON_FILE_NAME = TARGET_TABLE + \
                 "_" + SOURCE_SYSTEM  ##prt_persn_d_smax
STAGE_TABLE = ARGS['rs_stage_table']  ##prt_persn_d_smax_stage
print("stagedb_table" + str(STAGE_DATABASE_NAME) + "_" + str(STAGE_TABLE))
CTLG_CONNECTION = ARGS['glue_conn']  ##TestRedshift
REDSHIFTDB = ARGS['rs_db']  ##usinnovationredshift
S3_BUCKET = ARGS['bkt_name']  ##"odp-us-innovation-raw"
MD5_COLUMN = TARGET_TABLE + "_md5"  ##prt_persn_d_md5
TARGET_TABLE_COLUMNS = ARGS['target_cols']  ##As per DDL(col1,col2,col3)
STAGE_TABLE_COLUMNS = ARGS['stage_cols']  ##As per DDL(col1,col2,col3)
DBTABLE_STG = STAGE_DATABASE_NAME + "." + STAGE_TABLE

SC = SparkContext()
GLUECONTEXT = GlueContext(SC)
SPARK = GLUECONTEXT.spark_session
JOB = Job(GLUECONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)
RUN_ID = ARGS['JOB_RUN_ID']
JOB_NAME = ARGS['JOB_NAME']
TEMPDIR = ARGS['TempDir']

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

PRT_PERSN_D_SOURCE_DF = PRT_PERSN_D_SOURCE.toDF().filter("op_val in (0,1,2)")


def get_filtered_source_data(s_df1):
    """
    To get only proper Data and Required Fields that are used for
    Transformations Adding and Renaming the columns as per the
    Business Requirements.All GP function Logic is maintained here
    :param scon_df:
    :param ssrv_df:
    :param suser_df:
    :return final_ranked_df:
    """
    """
  Implementing business logic as per existing green plum code
  :param s_df1: PRT_PERSN_D_SOURCE_DF
  :return:source_prt_persn_d
  """


# Implementing business logic as per existing green plum code
    s_df1.createOrReplaceTempView("prt_persn_d_temp")
    source_prt_persn_d_df = SPARK.sql("""SELECT 
   '-99999' AS persn_idn, 
   CON.id AS persn_id, 
   '' AS persn_key,
   'CONTACT' AS persn_typ_key, 
   CON.firstname AS first_nam, 
   CON.gs_role__c AS role, 
   CON.email AS email_id, 
   CON.phone AS pohne_nbr, 
   CON.name AS full_nam, 
   CON.lastname AS last_nam, 
   '' AS fe_nam, 
   '' AS fe_id, 
   cast('' as decimal(18))  AS fe_idn, 
   '' AS hr_sso, 
   '' AS hr_nam, 
   '' AS mgr_sso, 
   '' AS mgr_nam, 
   'SVMXC' AS source_name, 
   'job_mrr_fnd_prt_persn_d_smax' AS posting_agent, 
   'servicemax.contact' AS data_origin, 
   current_timestamp AS load_dtm, 
   current_timestamp AS update_dtm, 
   cast(CON.lastmodifieddate as timestamp) AS source_update_dtm, 
   cast(CON.createddate as timestamp) AS source_creation_dtm, 
   'C' as versn_flg, 
   cast(CON.op_val as string) as op_val
   from prt_persn_d_temp as CON""")

    print("source_prt_persn_d_df:", source_prt_persn_d_df.count())
    print("source_prt_persn_d_df:", source_prt_persn_d_df.show(10))

    source_prt_persn_d_df = source_prt_persn_d_df \
    .withColumn('rank',
                F.row_number().over(Window.partitionBy("persn_id")
                                    .orderBy(F.desc('source_update_dtm')))) \
    .filter(F.col("rank") == 1)
    return source_prt_persn_d_df

print("Check1")


def prt_persn_d_df_insert(src_df):
    """
    Filtering New Inserted and updated records from Filtered source data
    :param src_df:
    :return ins_upd_final_df:
    """
    ins_upd_df = src_df.filter((src_df.op_val != 0) & (src_df.rank == 1))
    ins_upd_final_df = ins_upd_df \
        .withColumn('deleted_flag_ind', F.lit('N')) \
        .withColumn('current_flag_ind', F.lit('Y')) \
        .withColumn('end_dtm', F.lit('9999-12-31 23:59:59')
                    .astype('Timestamp')) \
        .withColumn('start_dtm', F.lit(DATETIMESTAMP)
                    .astype('Timestamp')) \
        .drop("rank")
    return ins_upd_final_df


def get_new_ins_upd_records(src_df):
    """
    Filtering New Inserted and updated records from Filtered source data
    :param src_df:
    :return ins_upd_final_df:
    """
    ins_upd_df = src_df.filter((src_df.op_val != 0) & (src_df.rank == 1))
    ins_upd_final_df = ins_upd_df \
        .withColumn('deleted_flag_ind', F.lit('N')) \
        .withColumn('current_flag_ind', F.lit('Y')) \
        .withColumn('end_dtm', F.lit('9999-12-31 23:59:59')
                    .astype('Timestamp')) \
        .withColumn('start_dtm', F.lit(DATETIMESTAMP)
                    .astype('Timestamp')) \
        .drop("rank")
    return ins_upd_final_df


def get_deleted_records(src_df):
    """
    Filtering Only deleted records from Filtered source data and
    Flag version is maintained in post query
    :param src_df:
    :return delete_df:
    """
    delete_df = src_df.filter((src_df.op_val == 0) &
                              (src_df.rank == 1)) \
        .withColumn('deleted_flag_ind', F.lit('Y')) \
        .withColumn('current_flag_ind', F.lit('N')) \
        .withColumn('end_dtm', F.lit(DATETIMESTAMP)
                    .astype('Timestamp')) \
        .withColumn('start_dtm', F.lit(DATETIMESTAMP)
                    .astype('Timestamp')) \
        .drop("rank")
    return delete_df


# Calling get_filtered_source_data Function
STEP_LOG = "Step 1:get_filtered_source_data()-Method-Initiated \n"
START_TIME = timeit.default_timer()
try:
    FILTERED_SRC_DF = get_filtered_source_data(PRT_PERSN_D_SOURCE_DF)
    # src_count = FILTERED_SRC_DF.count()
    # print("FILTERED_SRC_DF count" + str(src_count))
    FILTERED_SRC_DF.persist()
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    STEP_LOG = STEP_LOG + "Step 1:Succeeded \n" + "Step 1:Execution time: " + \
               str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)

    # calling get_new_ins_upd_records function for op_val=1,2
    STEP_LOG = STEP_LOG + "Step 2:get_new_ins_upd_records()-Method-Initiated \n"
    START_TIME = timeit.default_timer()
    INSERT_FINAL_DF = get_new_ins_upd_records(FILTERED_SRC_DF)
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    STEP_LOG = STEP_LOG + "Step 2:Succeeded \n" + "Step 2:Execution time: " + \
               str(ELAPSED_TIME) + "\n"
    # ins_upd_count = INSERT_FINAL_DF.count()
    # print("ins_upd_count count" + str(ins_upd_count))
    print(STEP_LOG)

    # calling get_deleted_records function for op_val=0
    STEP_LOG = STEP_LOG + "Step 3:get_deleted_records()-Method-Initiated \n"
    START_TIME = timeit.default_timer()
    DELETE_FINAL_DF = get_deleted_records(FILTERED_SRC_DF)
    # delete_count = DELETE_FINAL_DF.count()
    # print("delete_count count" + str(delete_count))
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    STEP_LOG = STEP_LOG + "Step 3:Succeeded \n" + "Step 3:Execution time: " + \
               str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)

    # Union of all the transformed records(Inserts ,Updates and Deletes)
    STEP_LOG = STEP_LOG + "Step 4:Final_Union_DataFrame-load-Initiated \n"
    START_TIME = timeit.default_timer()
    FINAL_UNION_DATAFRAME = INSERT_FINAL_DF \
        .unionAll(DELETE_FINAL_DF)
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    FILTERED_SRC_DF.unpersist()
    STEP_LOG = STEP_LOG + "Step 4:Succeeded \n" + "Step 4:Execution time:" + \
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
    STEP_LOG = STEP_LOG + "Step 5:getconfiguration() Method from " + \
               "configmd5 Framework-Initiated \n"
    START_TIME = timeit.default_timer()

    FINAL_TUPLE_WITH_DF_AND_MD5 = md5.getconfiguration(S3_BUCKET,
                                                       JSON_FILE_NAME,
                                                       FINAL_UNION_DATAFRAME,
                                                       MD5_COLUMN)
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    FINAL_MD5_DF = FINAL_TUPLE_WITH_DF_AND_MD5[0]

    # final_df_count = FINAL_MD5_DF.count()

    # print("final_df_count" +str(final_df_count))
    # Returns a List['Natural_Key']
    NATURAL_KEY = FINAL_TUPLE_WITH_DF_AND_MD5[1]
    # Taking the natural key that passed in Json File.
    NATURAL_KEY_1 = NATURAL_KEY[0]
    STEP_LOG = STEP_LOG + "Step 5:Succeeded \n" + "Step 5:Execution time:" + \
               str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)

except Exception as exception:
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    STEP_LOG = STEP_LOG + "Step :Failed with ERROR: " + \
               str(exception) + "\nStep :Execution time: " + \
               str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)

# Casting op_val to string
FINAL_MD5_DF = FINAL_MD5_DF \
    .withColumn("op_val",
                FINAL_MD5_DF["op_val"]
                .cast(StringType()))

# Final Data frame is converted to Dynamic frame
# Final Dynamic Frame will be written to Stage Table
FINAL_DYNAMIC_FRAME = DynamicFrame.fromDF(FINAL_MD5_DF,
                                          GLUECONTEXT,
                                          "FINAL_DYNAMIC_FRAME")

print("before_PRE_QUERY")
# Truncating the stage table
PRE_QUERY = """begin;
truncate table {stage_database_name}.{stage_table};
end;""".format(stage_database_name=STAGE_DATABASE_NAME,
               stage_table=STAGE_TABLE)

# print("after_PRE_QUERY" + str(PRE_QUERY))
# Implementing the SCD2 logic between the stage and the target table.
# UPDATES(SCD Type2) :md5_column != md5_column,Make inactive of
# active record in target_table using Stage Table.

# UPDATES(SCD Type1) :Update start_dtm in stage_table from target_table
# based on current matched condition(to handle time box)
# Now, md5_column = md5_column,Delete the matched
# existing records from target_table using Stage table.

# Inserts :Once all are handled as above,We will insert all latest records
# from stage table.

# DELETES : Op_val = 0,Soft deleting all versions based on
# primary_key of current batch in Target Table.

POST_QUERY = """begin;
update {target_database_name}.{target_table}
set end_dtm = {stage_table}.start_dtm - interval '1 second',
current_flag_ind = 'N'            
from {stage_database_name}.{stage_table}
where {stage_table}.{natural_key_1} = {target_table}.{natural_key_1}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.{md5_column} != {target_table}.{md5_column} 
and {target_table}.current_flag_ind = 'Y'
and {stage_table}.op_val = 2  ; 

update {stage_database_name}.{stage_table}
set start_dtm = {target_table}.start_dtm           
from {target_database_name}.{target_table}
where {stage_table}.{natural_key_1} = {target_table}.{natural_key_1}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.{md5_column} = {target_table}.{md5_column} 
and {target_table}.current_flag_ind = 'Y'
and {stage_table}.op_val =2  ; 


delete from {target_database_name}.{target_table} 
using
{stage_database_name}.{stage_table}
where {stage_table}.op_val = 2
and {stage_table}.{natural_key_1} = {target_table}.{natural_key_1}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.{md5_column} = {target_table}.{md5_column}
and {target_table}.current_flag_ind = 'Y'
and {target_table}.end_dtm = '9999-12-31 23:59:59';

insert into {target_database_name}.{target_table} 
({target_table_columns})
select {stage_table_columns} from {stage_database_name}.{stage_table}
where {stage_table}.op_val != 0;

Update {target_database_name}.{target_table} 
SET 
end_dtm = current_timestamp,
deleted_flag_ind = 'Y',
current_flag_ind='N'
from {stage_database_name}.{stage_table}
where 
{stage_table}.{natural_key_1} = {target_table}.{natural_key_1}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.op_val = 0;
end;""".format(target_database_name=TARGET_DATABASE_NAME,
               stage_database_name=STAGE_DATABASE_NAME,
               stage_table=STAGE_TABLE,
               target_table=TARGET_TABLE,
               natural_key_1=NATURAL_KEY_1,
               target_table_columns=TARGET_TABLE_COLUMNS,
               stage_table_columns=STAGE_TABLE_COLUMNS,
               md5_column=MD5_COLUMN)

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

JOB.commit()
