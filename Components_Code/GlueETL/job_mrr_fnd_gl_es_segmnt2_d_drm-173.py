"""
# Developer:madhavi Sastry susarla
# Type:SCD_TYPE_2
# Source Table1:HCBI_ES_HIER_ACCOUNT_ALT
# Source Table2:HCBI_ES_PROP_ACCOUNT_ALT
# Target_Table:gl_es_segment2_dh
# SourceSystem:DRM
# SourceSchema:DRM
###Description:
# This PySpark Code is used to migrate Data from Source Mirror Tables
# (HCBI_ES_HIER_ACCOUNT_ALT,HCBI_ES_PROP_ACCOUNT_ALT) to Target RedShift Table
# (gl_es_segment2_dh)and astaging table (gl_es_segment2_dh_staging)
# is also used in this process
# With added deleted_flag_ind column,which is set to 'Y'(op_val=0) at
# target for deleted record at source(For maintaining soft delete at target).
# And to 'N'(op_val=1) at target for New Inserted Records with
# New Surrogate Key generated in sequence.
# And to 'N'(op_val=2) at target for Updated Records with the
# existing Surrogate Key

  """

import sys
import timeit
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col
import Framework.conf_md5 as md5

## @params: [TempDir, JOB_NAME]
ARGS = getResolvedOptions(sys.argv,
                          ['TempDir', 'JOB_NAME', 'source_db',
                           'source_table_1', 'source_table_2',
                           'rs_target_schema', 'rs_stage_schema',
                           'source_system', 'rs_target_table',
                           'stage_cols', 'target_cols',
                           'glue_conn', 'rs_db',
                           'rs_stage_table', 'bkt_name'])

# Variables Declaration
# Variable Declaration for AWS GlueCatalog Source Data (DRM)
CTLG_CONNECTION = ARGS['glue_conn']
V_S_DATABASE = ARGS['source_db']
V_S_TABLE_NAME1 = ARGS['source_table_1']
V_S_TABLE_NAME2 = ARGS['source_table_2']

# Variable Declaration for AWS GlueCatalog for Target Data (DRM)
STAGE_TABLE_COLUMNS = ARGS['stage_cols']
TARGET_TABLE_COLUMNS = ARGS['target_cols']
TARGET_SCHEMA_NAME = ARGS['rs_target_schema']  ##datafabric_cfd_dea
STAGE_SCHEMA_NAME = ARGS['rs_stage_schema']
SOURCE_SYSTEM = ARGS['source_system']
T_DATABASE = ARGS['rs_db']
T_TABLE_NAME = ARGS['rs_target_table']
T_STAGE_TABLE_NAME = ARGS['rs_stage_table']
#T_STAGE_TABLE_NAME = STAGE_SCHEMA_NAME + "." + STAGE_TABLE
STAGE_TABLE = STAGE_SCHEMA_NAME + "." + T_STAGE_TABLE_NAME
#T_TABLE_NAME = TARGET_SCHEMA_NAME + "." + TARGET_TABLE
# JSON_FILE_NAME = TARGET_TABLE + \
#                 "_" + SOURCE_SYSTEM
S3_BUCKET = ARGS['bkt_name']
JSON_FILE_NAME = T_TABLE_NAME + \
                 "_" + SOURCE_SYSTEM.lower()
MD5_COLUMN = T_TABLE_NAME + "_md5"
MD5_COLUMN_SCD1 = T_TABLE_NAME + "_md5_scd1" 
#UNION_COLS = ARGS['cols_for_union']

# To generate the current time stamp
TIMESTAMP = datetime.timestamp(datetime.now())
CURRENT_DTM = datetime.fromtimestamp(TIMESTAMP)
ENDDTM = '9999-12-31 23:59:59'
JOB_START_TIME = timeit.default_timer()


SC = SparkContext()
GLUECONTEXT = GlueContext(SC)
SPARK = GLUECONTEXT.spark_session
JOB = Job(GLUECONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)
RUN_ID = ARGS['JOB_RUN_ID']
JOB_NAME = ARGS['JOB_NAME']
TEMPDIR = ARGS['TempDir']

## @type: DataSource
## @args: [database = "db_mirror_drm",
# table_name1= "source_table_name_hcbi_es_hier_account_alt",
# table_name2="source_table_name_hcbi_es_prop_account_alt",
# transformation_ctx = "hcbi_es_hier_account_alt_SOURCE"]
## @return: HCBI_ES_HIER_COST_CENTER_OR_SOURCE,
# HCBI_ES_PROP_COST_CENTER_OR_source
## @inputs: []
HCBI_ES_HIER_ACCOUNT_ALT_SOURCE = GLUECONTEXT.create_dynamic_frame \
    .from_catalog(database=V_S_DATABASE, table_name=V_S_TABLE_NAME1,
                  transformation_ctx="HCBI_ES_HIER_ACCOUNT_ALT_SOURCE")
HCBI_ES_HIER_ACCOUNT_ALT_SOURCE_DF = HCBI_ES_HIER_ACCOUNT_ALT_SOURCE.toDF()\
.select('MEMBER_NAME', 'MEMBER_DESC', 'CHILD', 'PARENT',\
'NODE_TYPE', 'node_status', 'MASTER_FLAG', \
'MASTER_FLAG_DESC', 'last_refresh', \
'hvr_last_upd_tms', 'op_val').dropDuplicates()
HCBI_ES_HIER_ACCOUNT_ALT_SOURCE_DF.cache()

# View is created for selecting the required columns from the source to insert
HCBI_ES_HIER_ACCOUNT_ALT_SOURCE_DF \
    .createOrReplaceTempView("HCBI_ES_HIER_ACCOUNT_ALT_SOURCE_DF_V")

HCBI_ES_PROP_ACCOUNT_ALT_SOURCE = GLUECONTEXT.create_dynamic_frame \
    .from_catalog(database=V_S_DATABASE, table_name=V_S_TABLE_NAME2,
                  transformation_ctx="HCBI_ES_PROP_ACCOUNT_ALT_SOURCE")
HCBI_ES_PROP_ACCOUNT_ALT_SOURCE_DF = HCBI_ES_PROP_ACCOUNT_ALT_SOURCE.toDF()\
.filter("node_type = 'Leaf'")\
.select('MEMBER_NAME').dropDuplicates()
HCBI_ES_PROP_ACCOUNT_ALT_SOURCE_DF.cache()

# Join Query to take records from the two source tables
# that satisfy the filter condition

COMMON_QUERY_DF = HCBI_ES_HIER_ACCOUNT_ALT_SOURCE_DF.alias("hier") \
    .join(HCBI_ES_PROP_ACCOUNT_ALT_SOURCE_DF
          .alias("prop"),
          (col("hier.child") == col("prop.MEMBER_NAME")), "inner") \
    .select('hier.MEMBER_NAME', 'hier.MEMBER_DESC', 'hier.CHILD')
COMMON_QUERY_DF.cache()
COMMON_QUERY_DF.createOrReplaceTempView("COMMON_QUERY_DF_V")




# Adding and Renaming the columns as per the Business Requirements.
def gl_es_segment2_ds_sourcedata():
    """
    selecting the desired columns from the two source tables
    :return: gl_es_segment2_ds_sourcedata_dframe
    """
    gl_es_segment2_ds_sourcedata_df = SPARK.sql(f"""
    SELECT
    COMMON_QUERY_DF_V.MEMBER_NAME AS account_key  ,
    COMMON_QUERY_DF_V.MEMBER_DESC AS account_desc  ,
    COMMON_QUERY_DF_V.CHILD AS alt_cc_nam, 
    HIER_1.NODE_TYPE AS levl_proprty,						
	HIER_12.MEMBER_NAME AS levl1_key,						
	HIER_12.CHILD AS levl1_id,						
	HIER_12.MEMBER_DESC AS levl1_desc,						
	HIER_11.MEMBER_NAME AS levl2_key,						
	HIER_11.CHILD AS levl2_id,						
	HIER_11.MEMBER_DESC AS levl2_desc,						
	HIER_10.MEMBER_NAME AS levl3_key,						
	HIER_10.CHILD AS levl3_id,						
	HIER_10.MEMBER_DESC AS levl3_desc,						
	HIER_9.MEMBER_NAME AS levl4_key,						
	HIER_9.CHILD AS levl4_id,						
	HIER_9.MEMBER_DESC AS levl4_desc,						
	HIER_8.MEMBER_NAME AS levl5_key,						
	HIER_8.CHILD AS levl5_id,						
	HIER_8.MEMBER_DESC AS levl5_desc,						
	HIER_7.MEMBER_NAME AS levl6_key,						
	HIER_7.CHILD AS levl6_id,						
	HIER_7.MEMBER_DESC AS levl6_desc,						
	HIER_6.MEMBER_NAME AS levl7_key,						
	HIER_6.CHILD AS levl7_id,						
	HIER_6.MEMBER_DESC AS levl7_desc,						
	HIER_5.MEMBER_NAME AS levl8_key,						
	HIER_5.CHILD AS levl8_id,						
	HIER_5.MEMBER_DESC AS levl8_desc,						
	HIER_4.MEMBER_NAME AS levl9_key,						
	HIER_4.CHILD AS levl9_id,						
	HIER_4.MEMBER_DESC AS levl9_desc,						
	HIER_3.MEMBER_NAME AS levl10_key,						
	HIER_3.CHILD AS levl10_id,						
	HIER_3.MEMBER_DESC AS levl10_desc,						
	HIER_2.MEMBER_NAME AS levl11_key,						
	HIER_2.CHILD AS levl11_id,						
	HIER_2.MEMBER_DESC AS levl11_desc,						
	HIER_1.MEMBER_NAME AS levl12_key,						
	HIER_1.CHILD AS levl12_id,						
	HIER_1.MEMBER_DESC AS levl12_desc,						
	HIER_1.node_status as node_status,						
	HIER_1.MASTER_FLAG as master_flg,						
	HIER_1.MASTER_FLAG_DESC as master_flg_desc,
	cast(HIER_1.op_val as string) AS op_val,
	HIER_1.hvr_last_upd_tms, 
	concat(coalesce(hier_1.MEMBER_NAME,'~','drm')) as src_sys_id,
	'DRM' AS source_name,	
	'job_mrr_fnd_gl_es_segmnt2_dh_drm'  AS  posting_agent,
	'HCBI_ES_HIER_ACCOUNT_ALT' AS data_origin ,
	CAST(HIER_1.last_refresh AS TIMESTAMP) AS source_update_dtm,
    CAST(HIER_1.last_refresh AS TIMESTAMP) AS source_creation_dtm,
    CAST('{CURRENT_DTM}' AS TIMESTAMP) AS load_dtm,
    CAST('{CURRENT_DTM}' AS TIMESTAMP) AS update_dtm
    from COMMON_QUERY_DF_V 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_1 
    ON COMMON_QUERY_DF_V.CHILD=HIER_1.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_2 ON HIER_1.PARENT = HIER_2.CHILD
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_3 ON HIER_2.PARENT =HIER_3.CHILD
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_4 ON HIER_3.PARENT =HIER_4.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_5 ON HIER_4.PARENT =HIER_5.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_6 ON HIER_5.PARENT =HIER_6.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_7 ON HIER_6.PARENT =HIER_7.CHILD
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_8 ON HIER_7.PARENT =HIER_8.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_9 ON HIER_8.PARENT =HIER_9.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_10 ON HIER_9.PARENT =HIER_10.CHILD 
    LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_11 ON HIER_10.PARENT =HIER_11.CHILD
	LEFT JOIN hcbi_es_hier_account_alt_SOURCE_Df_V HIER_12 
	ON HIER_11.PARENT =HIER_12.CHILD	""")

    gl_es_segment2_insert_df = gl_es_segment2_ds_sourcedata_df\
		.withColumn('rank', (F.row_number().over(Window.partitionBy('account_key', 'source_name').orderBy(F.desc('hvr_last_upd_tms'))))).drop("hvr_last_upd_tms")
    source_ranked_dfnl = gl_es_segment2_insert_df.filter(gl_es_segment2_insert_df.rank == 1)
    return source_ranked_dfnl

def get_new_ins_upd_records(src_df):
    """
    Filtering New Inserted and updated records from Filtered source data
    :param FILTERED_SRC_DF:
    :return ins_upd_final_df:
    """
    ins_upd_final_df = src_df \
        .withColumn('deleted_flag_ind', F.lit('N')) \
        .withColumn('current_flag_ind', F.lit('Y')) \
        .withColumn('end_dtm', F.lit('9999-12-31 23:59:59')
                    .astype('Timestamp')) \
        .withColumn('start_dtm', F.lit(CURRENT_DTM)
                    .astype('Timestamp')) \
        .drop("rank")
    return ins_upd_final_df

# gl_es_segment2_ds_insert Function for getting New Inserted Records at Source
# And adding "deleted_flag_ind" Column and setting it to
# 'N' for New Inserted Records.




# Calling gl_es_segment2_ds_sourcedata Function to get Source Data
# start logging information of the steps
STEP_LOG = "Step 1: gl_es_cost_center_org_hier-source-load-Initiated \n"
START_TIME = timeit.default_timer()
try:
    GL_ES_SEGMENT2_DS_SOURCEDATA_DF = gl_es_segment2_ds_sourcedata()
    GL_ES_SEGMENT2_DS_SOURCEDATA_DF.persist()
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    INPUT_RECORDS = GL_ES_SEGMENT2_DS_SOURCEDATA_DF.count()
    STEP_LOG = STEP_LOG + "Step 1: gl_es_segment2_ds-source-load-SUCCEEDED \n" + "Step 1:Execution time: " + \
                                                  str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)
except Exception as error:
	ELAPSED_TIME = timeit.default_timer() - START_TIME
	STEP_LOG = STEP_LOG + "Step 1: gl_es_segment2_ds-source-load-FAILED" \
                          " With ERROR" + str(error) + "\n" + "Step 1:Execution time: " + \
                                                  str(ELAPSED_TIME) + "\n"
print(STEP_LOG)
    # sys.exit()

# Calling gl_es_segment2_ds_insert_df Function to get New
# Inserted Records From Source
STEP_LOG = "Step 2: gl_es_segment2_ds_insert_del_df-source-load-Initiated \n"
START_TIME = timeit.default_timer()
try:
    INSERT_UPDATE_FINAL_DF =  get_new_ins_upd_records(GL_ES_SEGMENT2_DS_SOURCEDATA_DF)   
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    GL_ES_SEGMENT2_DS_SOURCEDATA_DF.unpersist()
    STEP_LOG = STEP_LOG + "Step 2: gl_es_segment2_ds_insert_del_" \
                          "df-source-load-SUCCEEDED \n" "Step 2:Execution time: " + \
                                                  str(ELAPSED_TIME) + "\n"
    print(STEP_LOG)
except Exception as error:
	ELAPSED_TIME = timeit.default_timer() - START_TIME
	STEP_LOG = STEP_LOG + "Step 2: gl_es_segment2_ds_insert_del_df-" \
                      "source-load-FAILED With ERROR " + str(error) + "\n" "Step 2:Execution time: " + \
                                                  str(ELAPSED_TIME) + "\n"
print(STEP_LOG)
# sys.exit()


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
													            INSERT_UPDATE_FINAL_DF,
                                                                MD5_COLUMN,
                                                                MD5_COLUMN_SCD1)
    ELAPSED_TIME = timeit.default_timer() - START_TIME
    FINAL_MD5_DF = FINAL_TUPLE_WITH_DF_AND_MD5[0]


    STEP_LOG = STEP_LOG + "Step 3:Succeeded \n" + "Step 3:Execution time: " + \
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
POST_QUERY_SOURCE_NAME = FINAL_MD5_DF.select("source_name").\
limit(1).rdd.map(lambda a: a[0]).collect()[0]

print('#######>>>>>>>POST_QUERY_SOURCE_NAME', POST_QUERY_SOURCE_NAME)

# Typecasting op_val before loading into Stage Table
FINAL_MD5_DF = FINAL_MD5_DF.withColumn("op_val",
                                       FINAL_MD5_DF["op_val"]
                                       .cast(StringType()))

# Final Data frame is converted to Dynamic frame
# Final Dynamic Frame will be written to Stage Table
FINAL_DYNAMIC_FRAME = DynamicFrame.fromDF(FINAL_MD5_DF,
                                          GLUECONTEXT,
                                          "Final_dynamic_frame")

#Updates,Inserts and Deletes counts logic here
# 1. Create a DF with counts and op_val, Group by JobId,op_val
# 2. Extract inserts, updates and deletes
# 3. Add it to Cloud Watch Logs.

COUNT_DF = FINAL_MD5_DF.withColumn('JobRunId', F.lit(str(RUN_ID)))\
                       .withColumn('JobName', F.lit(str(RUN_ID)))




PRE_QUERY = """begin;
truncate table {database_name}.{stage_table};
             end;""".format(database_name=STAGE_SCHEMA_NAME,
                            stage_table=T_STAGE_TABLE_NAME)

## Implementing the SCD2 logic between the stage and the target table.

## Files with op_val = 'T'

## Deletes : For op_val = 'T' , Soft delete the natural_key records present
## in target_table and not present in current batch stage_table

## No Change in stage_table record with opval = 'T', but present in target_table : 
## natural_key = natural_key & md5_column = md5_column & md5_column_scd1 = md5_column_scd1
## Hard delete records from stage_table which matched the above condition

## UPDATES(SCD Type2) : natural_key = natural_key &
## md5_column != md5_column & md5_column_scd1 = md5_column_scd1 ,
## Make inactive of active record in target_table using Stage Table.

## UPDATES(SCD Type2) : natural_key = natural_key &
## md5_column != md5_column & md5_column_scd1 != md5_column_scd1 ,
## Make inactive of active record in target_table using Stage Table.

## UPDATES(SCD Type1) :Update start_dtm in stage_table from target_table
## based on current matched condition(to handle time box)
## Now, natural_key = natural_key & md5_column = md5_column
## & md5_column_scd1 != md5_column_scd1 ,
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

end;""".format(target_database_name=TARGET_SCHEMA_NAME,
               stage_database_name=STAGE_SCHEMA_NAME,
               stage_table=T_STAGE_TABLE_NAME,
               target_table=T_TABLE_NAME,
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
                                        "dbtable":STAGE_TABLE,
                                        "database":T_DATABASE,
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
                       .withColumn('Job_start_time', F.lit(CURRENT_DTM))\
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

#FINAL_COUNTS_DYNAMIC_FRAME = DynamicFrame.fromDF(AUDITINGCOUNTSDF,
#                                                 glueContext,
#                                                 "final_counts_dynamic_frame")
#datasink5 = glueContext.write_dynamic_frame.\
#    from_jdbc_conf(frame=FINAL_COUNTS_DYNAMIC_FRAME,
#                   catalog_connection="TestMySQL",
#                   connection_options={"dbtable": "ntiauditing",
#                                       "database": "odpusinntidb"})


#Logging all the above info To Cloud Watch Logs
AUDITING_COUNTS_DF.show(10, False)

JOB.commit()
