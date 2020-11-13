"""
Developer:Sravan kumar,Email:sravan.kumar2@ge.com,SSOID:503171857
Type:SCD_TYPE_2
SourceSystem:must
SourceSchema:hcdrm_interface
Source tables:fse,syst,site
TargetSchema:datafabric_cds_dea
Target_Table:prt_persn_d
Description:
PySpark code is used to migrate data from source mirror tables
to target RedShift table.
And to maintain versions for type 2 attributes in target table.
"""
# setting up job configurations

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
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import lit, col
from pyspark.sql import types as T
# parameters
ARGS = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'source_db',
                                     'source_table',
                                     'source_table_1', 'source_table_2',
                                     'rs_db', 'rs_stage_schema',
                                     'rs_target_schema', 'rs_target_table',
                                     'rs_stage_table', 'glue_conn',
                                     'bkt_name', 'source_system'])


# Variable Declaration for AWS GlueCatalog Source data
S_DATABASE1 = ARGS['source_db']
#S_DATABASE2 = ARGS['source_db_2']
S_TABLE_NAME1 = ARGS['source_table']
S_TABLE_NAME2 = ARGS['source_table_1']
S_TABLE_NAME3 = ARGS['source_table_2']
SOURCE_SYSTEM = ARGS['source_system']

# Variable Declaration for AWS GlueCatalog
# for Target Data

TARGET_DATABASE_NAME = ARGS['rs_target_schema']
STAGE_DATABASE_NAME = ARGS['rs_stage_schema']
TARGET_TABLE = ARGS['rs_target_table']
JSON_FILE_NAME = TARGET_TABLE + \
                 "_" + SOURCE_SYSTEM
STAGE_TABLE = ARGS['rs_stage_table']
CTLG_CONNECTION = ARGS['glue_conn']
REDSHIFTDB = ARGS['rs_db']
S3_BUCKET = ARGS['bkt_name']  ##"odp-us-innovation-raw"
MD5_COLUMN = TARGET_TABLE + "_md5"
MD5_COLUMN_SCD1 = TARGET_TABLE + "_md5_scd1"


DBTABLE_STG = STAGE_DATABASE_NAME + "." + STAGE_TABLE

STAGE_COLUMNS ="""persn_idn, persn_id, persn_key, 
persn_typ_key,first_nam, role,
email_id,source_update_dtm,
source_creation_dtm,pohne_nbr, 
full_nam, last_nam, fe_nam, 
fe_id, fe_idn, source_name,
posting_agent, data_origin,
load_dtm, update_dtm, versn_flg, 
hr_sso, hr_nam, mgr_sso,
mgr_nam, prt_persn_d_md5, prt_persn_d_md5_scd1,
start_dtm, end_dtm, current_flag_ind,
deleted_flag_ind"""

TARGET_COLUMNS = """persn_idn, persn_id, persn_key, 
persn_typ_key,first_nam, role,
email_id,source_update_dtm,
source_creation_dtm,pohne_nbr, 
full_nam, last_nam, fe_nam, 
fe_id, fe_idn, source_name,
posting_agent, data_origin,
load_dtm, update_dtm, versn_flg, 
hr_sso, hr_nam, mgr_sso,
mgr_nam, prt_persn_d_md5, prt_persn_d_md5_scd1,
start_dtm, end_dtm, current_flag_ind,
deleted_flag_ind"""


# Initialize the GlueContext and SparkContext
SC = SparkContext()
GLUECONTEXT = GlueContext(SC)
SPARK = GLUECONTEXT.spark_session
JOB = Job(GLUECONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)
RUN_ID = ARGS['JOB_RUN_ID']
JOB_NAME = ARGS['JOB_NAME']
TEMPDIR = ARGS['TempDir']

#CurrentTimeStamp
DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
END_DTM = '9999-12-31 23:59:59'
JOB_START_TIME = timeit.default_timer()

# creating DataFrames from source glue crawlers

#SRC_NOTEMPTY = True


PRT_PERSN_D_SOURCE_1 = GLUECONTEXT.create_dynamic_frame. \
    from_catalog(database=S_DATABASE1, table_name=S_TABLE_NAME1,
                 transformation_ctx="PRT_PERSN_D_SOURCE_1")

PRT_PERSN_D_SOURCE_DF_1 = PRT_PERSN_D_SOURCE_1.toDF()

PRT_PERSN_D_SOURCE_2 = GLUECONTEXT.create_dynamic_frame. \
    from_catalog(database=S_DATABASE1, table_name=S_TABLE_NAME2,
                 transformation_ctx="PRT_PERSN_D_SOURCE_2")

PRT_PERSN_D_SOURCE_DF_2 = PRT_PERSN_D_SOURCE_2.toDF()

PRT_PERSN_D_SOURCE_3 = GLUECONTEXT.create_dynamic_frame. \
    from_catalog(database=S_DATABASE1, table_name=S_TABLE_NAME3,
                 transformation_ctx="PRT_PERSN_D_SOURCE_3")

PRT_PERSN_D_SOURCE_DF_3 = PRT_PERSN_D_SOURCE_3.toDF()



def get_filtered_source_data(source_df1, source_df2, source_df3):

   """
Implementing business logic as per existing green plum code
:param s_df1: PRT_PERSN_D_SOURCE_DF
:param s_df2: PRT_PERSN_D_SOURCE_DF
:param s_df3: PRT_PERSN_D
:return:PRT_PERSN_D
"""
   source_df1.createOrReplaceTempView("prt_persn_d_temp")
   source_df2.createOrReplaceTempView("prt_persn_d_temp_1")
   source_df3.createOrReplaceTempView("prt_persn_d_temp_2")
   source_prt_persn_d_1 = SPARK.sql(""" SELECT 
   '-99999' as persn_idn,
   FSE.CORESX||'~'||FSE.COTEC AS persn_id,
   FSE.CORESX||'~'||FSE.COTEC AS persn_key,
   'FE' AS persn_typ_key,
   FSE.NMPTEC AS first_nam,
   '' AS fe_nam,
   FSE.TELNUMD AS pohne_nbr,
   FSE.NMPTEC||'~'||FSE.NMTEC AS full_nam,
   FSE.NMTEC AS last_nam,
   FSE.COSSOTEC AS fe_id,
   'MUST' AS source_name,
   'job_mrr_fnd_prt_persn_d_must' AS posting_agent,
   'FSE' AS data_origin,
   cast(-99999 as decimal(18)) AS fe_idn,
   '' as role,
   '' as email_id,
   current_timestamp as source_update_dtm,
   current_timestamp as source_creation_dtm,
   current_timestamp as load_dtm,
   current_timestamp as update_dtm,
   'C' AS versn_flg,
   op_val,
   hvr_last_upd_tms
   FROM prt_persn_d_temp AS FSE """)
  

   source_prt_persn_d_2 = SPARK.sql(""" SELECT 
   '-99999' AS persn_idn,
   MUSTSYST.cosys||'~'||MUSTSYST.coresx AS persn_id,
   NULL AS persn_key,
   'CORESX~COSYS' AS persn_typ_key,
   MUSTSYST.NMRES AS first_nam,
   NULL AS fe_nam,
   MUSTSYST.TELNUM AS pohne_nbr,
   MUSTSYST.NMRES AS full_nam,
   NULL AS last_nam,
   NULL AS fe_id,
   'MUST' AS source_name,
   'job_mrr_fnd_prt_persn_d_must' AS posting_agent,
   'must.site-'||'must.job' AS data_origin,
   cast('' as decimal(18)) AS fe_idn,
   MUSTSYST.DSFONRES AS role,
   MUSTSYST.COEMAIL AS email_id,
   current_timestamp as source_update_dtm,
   current_timestamp as source_creation_dtm,
   current_timestamp as load_dtm,
   current_timestamp as update_dtm,
   'C' as versn_flg,
   MUSTSYST.op_val,
   MUSTSYST.hvr_last_upd_tms from 
   prt_persn_d_temp_1 MUSTSYST
   LEFT JOIN prt_persn_d_temp_2 MUSTSITE
   ON MUSTSYST.FK_SITECORESX=MUSTSITE.CORESX
   AND MUSTSYST.FK_SITECOSITE=MUSTSITE.COSITE """)

   source_prt_persn_d_1 = source_prt_persn_d_1 \
    .filter("op_val in ('T')") \
    .withColumn('rank',
                F.row_number().over(Window.partitionBy("persn_id", "source_name").
                                    orderBy(F.desc('hvr_last_upd_tms')))). \
    filter(F.col("rank") == 1).drop('rank')

   source_prt_persn_d_2 = source_prt_persn_d_2 \
    .filter("op_val in ('T')") \
    .withColumn('rank',
                F.row_number().over(Window.partitionBy("persn_id", "source_name")
                                    .orderBy(F.desc('hvr_last_upd_tms')))) \
       .filter(F.col("rank") == 1).drop('rank')

   print("\n\nSource DF 2: ", source_prt_persn_d_2.count())
   source_prt_persn_d_df = source_prt_persn_d_1.union(source_prt_persn_d_2)
   print("\n\nSource DF : ", source_prt_persn_d_df.count())
   source_prt_persn_d_df = source_prt_persn_d_df.withColumn("hr_sso", F.lit("")) \
    .withColumn("hr_nam", F.lit("")) \
    .withColumn("mgr_sso", F.lit("")) \
    .withColumn("mgr_nam", F.lit(""))

   source_prt_persn_d_df = source_prt_persn_d_df \
    .filter("op_val in ('T')") \
    .withColumn('rank',
                F.row_number().over(Window.partitionBy("persn_id", "source_name")
                                    .orderBy(F.desc('hvr_last_upd_tms')))) \
       .filter(F.col("rank") == 1).drop('rank')
   return source_prt_persn_d_df.distinct()


# Calling get_filtered_source_data Function
STEP_LOG = "Step 1:get_filtered_source_data()-Method-Initiated \n"
START_TIME = timeit.default_timer()
try:
    FILTERED_SRC_DF = get_filtered_source_data(PRT_PERSN_D_SOURCE_DF_1, PRT_PERSN_D_SOURCE_DF_2, PRT_PERSN_D_SOURCE_DF_3)
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

def get_new_ins_upd_records(source_df):

    """
    Filtering New Inserted and updated records from Filtered source data
    :param FILTERED_SRC_DF:
    :return ins_upd_final_df:
    """
    ins_upd_final_df = source_df \
        .withColumn('deleted_flag_ind', F.lit('N')) \
        .withColumn('current_flag_ind', F.lit('Y')) \
        .withColumn('end_dtm', F.lit('9999-12-31 23:59:59')
                    .astype('Timestamp')) \
        .withColumn('start_dtm', F.lit(DATETIMESTAMP)
                    .astype('Timestamp'))\
                    .drop("rank")

    return ins_upd_final_df

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
               target_table_columns=TARGET_COLUMNS,
               stage_table_columns=STAGE_COLUMNS,
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
