import csv
import pandas
import json
import boto3

filename = "Scripts/redshift_ddl_NPROD.csv"

df = pandas.read_csv(filename)
df = df.dropna(axis = 0, how = 'any')

size = len(df)
s3 = boto3.client('s3')
path_list = {}

for index in range(size):
    ddl_name = str(df['Name'][index])
    track = str(df['Track'][index])
    file_type = str(df['Type'][index])
    pr_id = str(df['PR_ID'][index])
    folder_name = ddl_name[:-5]
    file_ext = '.ddl'
    path_prefix = 'ODP/'
    path_postfix = '/src/redshift/tables'+'/'+folder_name+'/'+ddl_name+file_ext
    comp_path = pr_id+'@#'+path_prefix+track+path_postfix
    path_list[ddl_name] = comp_path

if len(path_list)>=0:        
    with open('manifest_ddl_git_info.json', 'w',encoding = 'UTF-8') as outfile:
        json.dump(path_list, outfile)
    #print("SP Log Info : --")
    #print(path_list)
    with open('manifest_ddl_git_info.json', 'w',encoding = 'UTF-8') as outfile:
        json.dump(path_list,outfile)
    with open('manifest_ddl_git_info.json', 'rb') as data:
        s3.upload_fileobj(data, 'odp-us-prod-raw', 'servicesuite/ODP/manifest/manifest_ddl_git_info.json')

response = s3.get_object(
    Bucket='odp-us-prod-raw',
    Key='servicesuite/ODP/manifest/manifest_ddl_order.json'
)
file_content = response['Body'].read().decode('utf-8')
json_content = json.loads(file_content)

if len(path_list)==0 and len(json_content)==0:
    print("FAILURE")
else:
    print("SUCCESS")