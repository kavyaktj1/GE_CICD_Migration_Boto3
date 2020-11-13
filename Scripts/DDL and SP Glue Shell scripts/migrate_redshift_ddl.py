import boto3
import json
import pandas
import threading
import time
import sys
from io import StringIO
from csv import writer
from pgdb import connect
from datetime import datetime
from awsglue.utils import getResolvedOptions


def get_manifest_file():
    s3 = boto3.resource('s3', region_name='us-east-1')
    location_obj = s3.Object("odp-us-prod-raw",
                             "servicesuite/ODP/manifest/manifest_ddl_git_info.json")
    manifest_file = json.loads((location_obj.get()['Body'].read().decode('utf-8')))
    return manifest_file
    
def get_pre_manifest_file():
    s3 = boto3.resource('s3', region_name='us-east-1')
    location_obj = s3.Object("odp-us-prod-raw",
                             "servicesuite/ODP/manifest/manifest_ddl_order.json")
    manifest_file = json.loads((location_obj.get()['Body'].read().decode('utf-8')))
    return manifest_file


def comp_git_changelog(manifest):
    try:
        del_items = {}
        exec_items = {}
        con = connect( host='us-prod-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com', user=REDSHIFT_USERNAME, password=REDSHIFT_PASSWORD, database='gehc_data',port= '5439' )
        cur = con.cursor()
        query = "select ddl_script_name,max(commit_id) from hdl_services_fnd_data.change_log where status='EXECUTED' and commit_id is not null group by ddl_script_name;"
        res = cur.execute(query)
        rows = res.fetchall()
        for row in rows:
            del_items[row[0]]=row[1]
        for key,value in dict(manifest).items():
            table_name_with_version = key
            pr_id = value.split('@#')[0]
            version_id_str = table_name_with_version.split('.')[-1]
            table_name = table_name_with_version.replace('.'+version_id_str,'')
            version_id = int(version_id_str)
            if table_name in del_items.keys():
                query_var = del_items[table_name]
                if version_id>query_var:
                    exec_items[key] = value
            else:
                exec_items[key] = value
        con.commit()
        cur.close()
        con.close()
        return exec_items
    except Exception as e:
        print('EXCEPTION : ',e)

def insert_backup_changelog(pr_id,table_name,timestamp,bkuptime,table_version,cur,flag):
    try:
        rev_time_list = timestamp.split('-')
        rev_time = rev_time_list[2]+'-'+rev_time_list[1]+'-'+rev_time_list[0]
        state_log_query = "INSERT INTO hdl_services_fnd_data.change_log VALUES(%s,%s,%s,%s, 'EXECUTED')";
        backup_log_query = "INSERT INTO hdl_services_fnd_data.backup_log VALUES(%s,%s,%s,%s, 'EXECUTED')";
        cur.execute(state_log_query,(pr_id,table_version,table_name,rev_time))
        if flag:
            backup_table = table_name+'_'+pr_id+'_'+bkuptime
            cur.execute(backup_log_query,(pr_id,table_name,backup_table,timestamp))
    except Exception as e:
        print('EXCEPTION : ',e)

def execute_backup_creation(pr_id,form_time,table_name,time_obj,table_version,cur):
    try:
        flag = True
        lock_q = "LOCK {main_table};".format(main_table=table_name)
        table_creation_q = "CREATE TABLE {main_table}_{PR}_{Time}(LIKE {main_table});".format(main_table=table_name,PR=pr_id,Time=time_obj)
        table_insert_q = "INSERT INTO {main_table}_{PR}_{Time} SELECT * FROM {main_table};".format(main_table=table_name,PR=pr_id,Time=time_obj)
        table_compare_q = "SELECT EXISTS(SELECT COUNT(*) AS c1 FROM {main_table} HAVING c1 IN (SELECT COUNT(*) FROM {main_table}_{PR}_{Time}));".format(main_table=table_name,PR=pr_id,Time=time_obj)
        cur.execute(lock_q)
        cur.execute(table_creation_q)
        print('Backup Creation Executed')
        cur.execute(table_insert_q)
        print('Backup Insertion Executed')
        time.sleep(5)
        res = cur.execute(table_compare_q)
        for row in res:
            bool_flag = row[0]
        time_checker = 0
        while bool_flag!=True and time_checker<=5:
            time.sleep(5)
            time_checker+=1
            res = cur.execute(query)
            for row in res:
                bool_flag = row[0]
        if bool_flag==False:
            cur.execute("rollback;")
            flag = False
            print("Table Mismatch Error: Transaction Rollback")
            state_log_query = "INSERT INTO hdl_services_fnd_data.change_log VALUES(%s,%s,%s,%s, 'FAILED')";
            rev_time_list = timestamp.split('-')
            rev_time = rev_time_list[2]+'-'+rev_time_list[1]+'-'+rev_time_list[0]
            cur.execute(state_log_query,(pr_id,table_version,table_name,rev_time))
            SKIP_LIST.append(table_name)
            cur.execute('commit;')
            cur.execute('end;')
        else:
            print('Backup and Main Tables are matching')
            cur.execute('commit;')
            cur.execute('end;')
        return flag
    except Exception as e:
        print('EXCEPTION : ',table_name,' ',e)
        cur.execute("rollback;")
        print('Executing Rollback')
        state_log_query = "INSERT INTO hdl_services_fnd_data.change_log VALUES(%s,%s,%s,%s, 'FAILED')";
        rev_time_list = timestamp.split('-')
        rev_time = rev_time_list[2]+'-'+rev_time_list[1]+'-'+rev_time_list[0]
        cur.execute(state_log_query,(pr_id,table_version,table_name,rev_time))
        cur.execute('commit;')
        cur.execute('end;')
        SKIP_LIST.append(table_name)

def execute_ddl(queries,pr_id,timestamp,table_name,bkuptime,table_version,cur,flag):
    try:
        for query in queries:
            query = query.strip()
            if len(query)!=0:
                query = query+';'
                cur.execute(query)
                print('Query Executed : ',query)
        insert_backup_changelog(pr_id,table_name,timestamp,bkuptime,table_version,cur,flag)
        return True
    except Exception as e:
        print('EXCEPTION : ',table_name,' ',e)
        if flag==False:
            cur.execute("rollback;")
            cur.execute('commit;')
            cur.execute('end;')
        else:
            drop_bkup_query = "DROP TABLE {main_table}_{PR}_{Time};".format(main_table=table_name,PR=pr_id,Time=bkuptime)
            cur.execute(drop_bkup_query)
            print('Dropping the backup table')
        state_log_query = "INSERT INTO hdl_services_fnd_data.change_log VALUES(%s,%s,%s,%s, 'FAILED')";
        rev_time_list = timestamp.split('-')
        rev_time = rev_time_list[2]+'-'+rev_time_list[1]+'-'+rev_time_list[0]
        cur.execute(state_log_query,(pr_id,table_version,table_name,rev_time))
        SKIP_LIST.append(table_name)
        return False

def get_s3_objects(file_loc):
    try:
        s3 = boto3.resource('s3', region_name='us-east-1')
        print(file_loc)
        location_obj = s3.Object("odp-us-prod-raw",
                                 file_loc)
        s3_file = location_obj.get()['Body'].read().decode('utf-8')
        return s3_file
    except Exception as e:
        print('File not found exception : ',file_loc)
        return 'FileNotFound'

if __name__ == '__main__':
    try:
        global REDSHIFT_USERNAME, REDSHIFT_PASSWORD, SKIP_LIST
        #SECRET = get_redshift_credentials()
        #REDSHIFT_USERNAME = '502819866'
        #REDSHIFT_PASSWORD = '$1$tLn.Jspl$qiwV'
        REDSHIFT_USERNAME = '502818085'
        REDSHIFT_PASSWORD = 'gAcw@xWKz6ZR'
        SKIP_LIST = [] # Contains the tables that won't be executed because of previous version rollback of same table. Ex- 0002,0003 will be skipped because of 0001 rollback.
        print('Before manifest file')
        manifest_file_git = get_manifest_file()
        manifest_file_pre = get_pre_manifest_file()
        print('File found')
        manifest_pre = comp_git_changelog(manifest_file_pre) 
        print('Manifest file updated')
        print(manifest_pre)
        print(manifest_file_git)
        file_check_var = not bool(manifest_file_git)
        if file_check_var:
            print('No DDLs to execute')
        con = connect( host='us-prod-redshift.cjn5iynjwss7.us-east-1.redshift.amazonaws.com', user=REDSHIFT_USERNAME, password=REDSHIFT_PASSWORD, database='gehc_data',port= '5439' )
        cur = con.cursor()
        for table_name_with_version,comp_path_with_pr in manifest_pre.items():
            pr_id = comp_path_with_pr.split('@#')[0]
            comp_path = comp_path_with_pr.split('@#')[1]
            s3_path = 'servicesuite/'+comp_path
            file_flag = 'NORMAL'
            s3_obj = get_s3_objects(s3_path)
            if s3_obj!='FileNotFound':
                table_version_str = table_name_with_version.split('.')[-1]
                table_name = table_name_with_version.replace('.'+table_version_str,'')
                if table_name not in SKIP_LIST:
                    table_version = int(table_version_str)
                    curr_time = datetime.now()
                    time_obj = curr_time.strftime("%d_%b_%Y_%H_%M")
                    form_time = curr_time.strftime("%d-%m-%Y")
                    list_str = []
                    cur.execute("begin;")
                    #for line in s3_obj:
                        #line_str = line.decode('utf-8')
                        #line_str = line_str.strip()
                        #if line_str.startswith('#'):
                        #    continue
                        #else:
                        #    list_str.append(line_str)
                    list_str = s3_obj.split(';')
                    for query in list_str:
                        if 'ALTER TABLE' in query or 'alter table' in query:
                            file_flag = 'ALTER'
                            break
                    if file_flag=='ALTER':
                        bool_flag = execute_backup_creation(pr_id,form_time,table_name,time_obj,table_version,cur)
                        if bool_flag:
                            ddl_flag = execute_ddl(list_str,pr_id,form_time,table_name,time_obj,table_version,cur,True)
                    else:
                        ddl_flag = execute_ddl(list_str,pr_id,form_time,table_name,time_obj,table_version,cur,False)
                    if ddl_flag==True:
                        cur.execute("commit;")
                        cur.execute("end;")
                else:
                    print("For Table : ",table_name_with_version," previous table DDL failed. Other versions won't be executed")
        manifest_file = comp_git_changelog(manifest_file_git)
        for table_name_with_version,comp_path_with_pr in manifest_file.items():
            pr_id = comp_path_with_pr.split('@#')[0]
            comp_path = comp_path_with_pr.split('@#')[1]
            s3_path = 'servicesuite/'+comp_path
            file_flag = 'NORMAL'
            s3_obj = get_s3_objects(s3_path)
            if s3_obj!='FileNotFound':
                table_version_str = table_name_with_version.split('.')[-1]
                table_name = table_name_with_version.replace('.'+table_version_str,'')
                if table_name not in SKIP_LIST:
                    table_version = int(table_version_str)
                    curr_time = datetime.now()
                    time_obj = curr_time.strftime("%d_%b_%Y_%H_%M")
                    form_time = curr_time.strftime("%d-%m-%Y")
                    list_str = []
                    cur.execute("begin;")
                    #for line in s3_obj:
                        #line_str = line.decode('utf-8')
                        #line_str = line_str.strip()
                        #if line_str.startswith('#'):
                        #    continue
                        #else:
                        #    list_str.append(line_str)
                    list_str = s3_obj.split(';')
                    for query in list_str:
                        if 'ALTER TABLE' in query or 'alter table' in query:
                            file_flag = 'ALTER'
                            break
                    if file_flag=='ALTER':
                        bool_flag = execute_backup_creation(pr_id,form_time,table_name,time_obj,table_version,cur)
                        if bool_flag:
                            ddl_flag = execute_ddl(list_str,pr_id,form_time,table_name,time_obj,table_version,cur,True)
                    else:
                        ddl_flag = execute_ddl(list_str,pr_id,form_time,table_name,time_obj,table_version,cur,False)
                    if ddl_flag==True:
                        cur.execute("commit;")
                        cur.execute("end;")
                else:
                    print("For Table : ",table_name_with_version," previous table DDL failed. Other versions won't be executed")
        con.commit()
        cur.close()
        con.close()
        print("Failed Jobs : ",SKIP_LIST)
        if len(SKIP_LIST)==0:
            print('SUCCESS')
        else:
            print('FAILURE')
    except Exception as e:
        print(e)