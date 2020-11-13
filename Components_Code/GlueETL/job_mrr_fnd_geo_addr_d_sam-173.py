"""SAM GEO_ADDR_D Table Migration SCD TYPE-2"""
import sys
import boto3 
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
import Framework.conf_md5 as md5


# Developer: Bhaskara Venkatesh ,Email:Bhaskara.Venkatesh@ge.com
# Type:SCD_TYPE_2
# Target_Table:GEO_ADDR_D
# SourceSystem:db_mrr_siebel_americas
# SourceTables:s_addr_per,s_addr_per_x,s_zipcode
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

## @params: [TempDir, JOB_NAME]
ARGS = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'source_db',
                                     'source_table_1', 'source_table_2',
                                     'source_table_3', 'rs_target_schema',
                                     'rs_stage_schema', 'source_system',
                                     'rs_target_table', 'rs_stage_table',
                                     'stage_cols',
                                     'target_cols', 'glue_conn',
                                     'rs_db', 'bkt_name'])

# Variable Declaration
SRC_DB = ARGS['source_db']  ##db_mrr_siebel_americas
SRC_TABLE_1 = ARGS['source_table_1']  ##s_addr_per
SRC_TABLE_2 = ARGS['source_table_2']  ##s_addr_per_x
SRC_TABLE_3 = ARGS['source_table_3']  ##s_zipcode
TARGET_DATABASE_NAME = ARGS['rs_target_schema']  ##datafabric_cfd_dea
STAGE_DATABASE_NAME = ARGS['rs_stage_schema']  ##datafabric_cfd_dea
TARGET_TABLE = ARGS['rs_target_table']  ##geo_addr_d
SOURCE_SYSTEM = ARGS['source_system']  ##sam
JSON_FILE_NAME = TARGET_TABLE + \
                 "_" + SOURCE_SYSTEM  ##geo_addr_d_sam
STAGE_TABLE = ARGS['rs_stage_table']  ##geo_addr_d_sam_stage
CTLG_CONNECTION = ARGS['glue_conn']  ##TestRedshift1
REDSHIFTDB = ARGS['rs_db']  ##usinnovationredshift
S3_BUCKET = ARGS['bkt_name']  ##"odp-us-innovation-raw"
MD5_COLUMN = TARGET_TABLE + "_md5"  ##geo_addr_d_md5
TARGET_TABLE_COLUMNS = ARGS['target_cols']  ##As per DDL(col1,col2,col3)
STAGE_TABLE_COLUMNS = ARGS['stage_cols']  ##As per DDL(col1,col2,col3)
DBTABLE_STG = STAGE_DATABASE_NAME + "." + STAGE_TABLE
DATETIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

SC = SparkContext()
GLUECONTEXT = GlueContext(SC)
SPARK = GLUECONTEXT.spark_session
JOB = Job(GLUECONTEXT)
JOB.init(ARGS['JOB_NAME'], ARGS)
RUN_ID = ARGS['JOB_RUN_ID']
JOB_NAME = ARGS['JOB_NAME']
TEMPDIR = ARGS['TempDir']

## @type: DataSource
## @args: [database = "db_mrr_siebel_americas",
## table_name = "s_contact",
## transformation_ctx = "sam_s_addr_per_dyf"]
## @return: DynamicFrame
## @inputs: []
SAM_S_ADDR_PER_DYF = GLUECONTEXT.create_dynamic_frame \
    .from_catalog(database=SRC_DB,
                  table_name=SRC_TABLE_1,
                  transformation_ctx="sam_s_addr_per_dyf")
SAM_S_ADDR_PER_DF = SAM_S_ADDR_PER_DYF.toDF().filter("op_val in (0,1,2)")
## @type: DataSource
## @args: [database = "db_mrr_siebel_americas",
## table_name = "s_srv_req",
## transformation_ctx = "sam_s_addr_per_x_dyf"]
## @return: DynamicFrame
## @inputs: []
SAM_S_ADDR_PER_X_DYF = GLUECONTEXT.create_dynamic_frame \
    .from_catalog(database=SRC_DB,
                  table_name=SRC_TABLE_2,
                  transformation_ctx="sam_s_addr_per_x_dyf")
SAM_S_ADDR_PER_X_DF = SAM_S_ADDR_PER_X_DYF.toDF().filter("op_val in (0,1,2)")

## @type: DataSource
## @args: [database = "db_mrr_siebel_americas",
## table_name = "s_user",
## transformation_ctx = "sam_s_zipcode_dyf"]
## @return: DynamicFrame
## @inputs: []
SAM_S_ZIPCODE_DYF = GLUECONTEXT.create_dynamic_frame \
    .from_catalog(database=SRC_DB,
                  table_name=SRC_TABLE_3,
                  transformation_ctx="sam_s_zipcode_dyf")
SAM_S_ZIPCODE_DF = SAM_S_ZIPCODE_DYF.toDF().filter("op_val in (0,1,2)")


def get_filtered_source_data(s_addr_per_df, s_addr_per_x_df, s_zipcode_df):
    """
    To get only proper Data and Required Fields that are used for
    Transformations Adding and Renaming the columns as per the
    Business Requirements.All GP function Logic is maintained here
    :param s_addr_per_df:
    :param s_addr_per_x_df:
    :param s_zipcode_df:
    :return final_ranked_df:
    """
    s_addr_per_df.createOrReplaceTempView("S_ADDR_PER")
    s_addr_per_x_df.createOrReplaceTempView("S_ADDR_PER_X")
    s_zipcode_df.createOrReplaceTempView("S_ZIPCODE")

    geo_addr_sam_df = SPARK.sql("""
    SELECT 
    ADDR_PER.row_id AS addr_id,
    cast('-99999' as decimal(38,4)) as addr_idn,
    cast(NULL as string) AS addr_key,
    'MAIN_ADDRESS' AS addr_typ,
    ADDR_PER.addr AS addr1,
    ADDR_PER.addr_line_2 AS addr2,
    ADDR_PER.addr_line_3 AS addr3,
    ADDR_PER.addr_line_4 AS addr4,
    ADDR_PER.city AS city,
    ADDR_PER.country AS country,
    ADDR_PER.county AS county,
    'S_ADDR_PER~S_ADDR_PER_X' AS data_origin,
    current_timestamp as load_dtm ,
    current_timestamp as update_dtm,
    cast(NULL as string) AS pole,
    ADDR_PER.zipcode AS postl_cd,
    'job_mrr_fnd_geo_addr_d_sam'AS posting_agent,
    ADDR_PER.province AS provnc,
    ADDR_PER.country_region AS regn,
    ADDR_PER.created AS source_creation_dtm,
    'SAM' AS source_name, 
    ADDR_PER.last_upd AS source_update_dtm,
    ADDR_PER.state AS state,
    'C' AS VERSN_FLG,
    ADDR_PER.time_zone_cd AS zone,
    ADDR_PER_X.ATTRIB_41 AS site_id,
    ADDR_PER.x_ucm_id_addr as ucmid_ai,
    ADDR_PER.x_ucm_site_id_addr as ucm_site_id,

    --US44636 columns added -----------
    ADDR_PER_X.ATTRIB_03 AS Facilityid ,
    ADDR_PER_X.ATTRIB_41 AS ERPSiteNumber,
    ------------US44636---------

    cast(MAX(ZIPCD.LATITUDE) as float) AS SYST_LATITUDE,
    cast(MAX(ZIPCD.LONGITUDE) as float) AS SYST_LONGITUDE,
    ADDR_PER.hvr_last_upd_tms as hvr_last_upd_tms,
    ADDR_PER.op_val as op_val
    FROM 
    S_ADDR_PER AS ADDR_PER
    LEFT JOIN S_ADDR_PER_X AS ADDR_PER_X 
    ON  ADDR_PER.ROW_ID = ADDR_PER_X.PAR_ROW_ID
    LEFT JOIN S_ZIPCODE ZIPCD
    ON ADDR_PER.ZIPCODE=ZIPCD.ZIPCODE
    group by ZIPCD.ZIPCODE,addr_per.row_id,addr_per.addr,
    addr_per.addr_line_2,addr_per.addr_line_3,
    addr_per.addr_line_4,addr_per.city,
    addr_per.country,addr_per.county,
    addr_per.zipcode,addr_per.province
    ,addr_per.country_region,addr_per.created_by,addr_per.created,
    addr_per.last_upd_by,addr_per.last_upd,addr_per.state,
    addr_per.time_zone_cd,addr_per_x.attrib_41,
    addr_per.x_ucm_id_addr,addr_per.x_ucm_site_id_addr,
    ADDR_PER_X.ATTRIB_03,ADDR_PER.hvr_last_upd_tms,ADDR_PER.op_val""")

    final_ranked_df = geo_addr_sam_df \
        .withColumn('rank', F.row_number()
                    .over(Window.partitionBy('addr_id', 'source_name')
                          .orderBy(F.desc('hvr_last_upd_tms')))) \
        .drop("hvr_last_upd_tms")
    return final_ranked_df


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
STEP_LOG = "Step 1: get_filtered_source_data-load-Initiated \n"
try:
    FILTERED_SRC_DF = get_filtered_source_data(SAM_S_ADDR_PER_DF,
                                               SAM_S_ADDR_PER_X_DF,
                                               SAM_S_ZIPCODE_DF)
    FILTERED_SRC_DF.persist()
    STEP_LOG = STEP_LOG + "Step 1: get_filtered_source_data-load-SUCCEEDED \n"
    print(STEP_LOG)
except Exception as exception:
    STEP_LOG = STEP_LOG + "Step 1: get_filtered_source_data-load-FAILED " + \
               "With ERROR " + str(exception) + "\n"
    print(STEP_LOG)

# calling get_new_ins_upd_records function for op_val=1,2
STEP_LOG = "Step 2: get_new_ins_upd_records-load-Initiated \n"
try:
    INSERT_FINAL_DF = get_new_ins_upd_records(FILTERED_SRC_DF)
    STEP_LOG = STEP_LOG + "Step 2: get_new_inserted_records-load-SUCCEEDED  \n"
    print(STEP_LOG)
except Exception as exception:
    STEP_LOG = STEP_LOG + "Step 2: get_new_ins_upd_records-load-FAILED " + \
               "With ERROR " + str(exception) + "\n"
    print(STEP_LOG)

# calling get_deleted_records function for op_val=0
STEP_LOG = "Step 3: get_deleted_records-load-Initiated \n"
try:
    DELETE_FINAL_DF = get_deleted_records(FILTERED_SRC_DF)
    STEP_LOG = STEP_LOG + "Step 3: get_deleted_records-load-SUCCEEDED \n"
    print(STEP_LOG)
except Exception as exception:
    STEP_LOG = STEP_LOG + "Step 3: get_deleted_records-load-FAILED" + \
               " With ERROR " + str(exception) + "\n"
    print(STEP_LOG)

# Union all the transformed records(Inserts ,Updates and Deletes)
STEP_LOG = "Step 4: Final_Union_DataFrame-load-Initiated \n"
try:
    FINAL_UNION_DATAFRAME = INSERT_FINAL_DF \
        .unionAll(DELETE_FINAL_DF)
    STEP_LOG = STEP_LOG + "Step 4: Final_Union_DataFrame-load-SUCCEEDED  \n"
    print(STEP_LOG)
except Exception as exception:
    STEP_LOG = STEP_LOG + "Step 4: Final_Union_DataFrame-load-FAILED " + \
               "With ERROR " + str(exception) + "\n"
    print(STEP_LOG)

# Final DF with md5 column generation for SCD_TYPE2 Columns
# FINAL_TUPLE_WITH_DF_AND_MD5 returns Tuple containing
# DataFrame with md5 col and a Business Key.
STEP_LOG = "Step 5: FINAL_TUPLE_WITH_DF_AND_MD5-load-Initiated \n"
try:
    #Old md5.getconfiguration
    FINAL_TUPLE_WITH_DF_AND_MD5 = md5.getconfiguration(S3_BUCKET,
                                                       JSON_FILE_NAME,
                                                       FINAL_UNION_DATAFRAME,
                                                       MD5_COLUMN)
    FINAL_MD5_DF = FINAL_TUPLE_WITH_DF_AND_MD5[0]
    FILTERED_SRC_DF.unpersist()
    STEP_LOG = STEP_LOG + "Step 5: FINAL_TUPLE_WITH_DF_AND_MD5-load-" + \
               "SUCCEEDED  \n"
    print(STEP_LOG)
except Exception as exception:
    STEP_LOG = STEP_LOG + "Step 5: FINAL_TUPLE_WITH_DF_AND_MD5-load-" \
                          "FAILED With ERROR " + str(exception) + "\n"
    print(STEP_LOG)

NATURAL_KEY = FINAL_TUPLE_WITH_DF_AND_MD5[1]
NATURAL_KEY_1 = NATURAL_KEY[0]

# Final Data frame is converted to Dynamic frame
FINAL_DYNAMIC_FRAME = DynamicFrame.fromDF(FINAL_MD5_DF,
                                          GLUECONTEXT,
                                          "Final_dynamic_frame")

## Truncating the stage table
PRE_QUERY = """begin;
truncate table {stage_database_name}.{stage_table};
end;""".format(stage_database_name=STAGE_DATABASE_NAME,
               stage_table=STAGE_TABLE)

## Implementing the SCD2 logic between the stage and the target table.
## UPDATES(SCD Type2) :md5_column != md5_column,Make inactive of
## active record in target_table using Stage Table.

## UPDATES(SCD Type1) :Update start_dtm in stage_table from target_table
## based on current matched condition(to handle time box)
## Now, md5_column = md5_column,Delete the matched
## records from target_table using Stage table.

## Inserts :Once all are handled as above,We will insert all latest records
## from stage table.

## DELETES : Op_val = 0,Soft deleting all versions based on
## primary_key of current batch in Target Table.

POST_QUERY = """begin;
update {target_database_name}.{target_table}
set end_dtm = {stage_table}.start_dtm - interval '1 second',
current_flag_ind = 'N'            
from {stage_database_name}.{stage_table}
where {stage_table}.{natural_key_1} = {target_table}.{natural_key_1}
and {stage_table}.source_name = {target_table}.source_name
and {stage_table}.{md5_column} != {target_table}.{md5_column} 
and {target_table}.current_flag_ind = 'Y'
and {stage_table}.op_val =2  ; 

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
where {stage_table}.op_val !=0;

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

FINAL_LOAD = GLUECONTEXT \
    .write_dynamic_frame \
    .from_jdbc_conf(frame=FINAL_DYNAMIC_FRAME,
                    catalog_connection=CTLG_CONNECTION,
                    connection_options={"preactions": PRE_QUERY,
                                        "dbtable": DBTABLE_STG,
                                        "database": REDSHIFTDB,
                                        "postactions": POST_QUERY},
                    redshift_tmp_dir=TEMPDIR,
                    transformation_ctx="FINAL_LOAD")

JOB.commit()
