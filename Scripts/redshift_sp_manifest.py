import csv
import pandas
import json
import boto3

filename = "Scripts/redshift_sp.csv"

df = pandas.read_csv(filename)
df = df.dropna(axis = 0, how = 'any')

size = len(df)

s3 = boto3.client('s3')
    
path_list = []

for index in range(size):
    sp_name = df['Name'][index]
    track = df['Track'][index]
    file_type = df['Type'][index]
    file_ext = ''
    if file_type=='sp':
        file_ext='.sp'
    elif file_type=='view':
        file_ext='.view'
    elif file_type=='dbscript':
        file_ext='.scr'
    path_prefix = 'ODP/'
    path_postfix = '/src/redshift/'+file_type+'/'+sp_name+file_ext
    comp_path = path_prefix+track+path_postfix
    path_list.append(comp_path)

if len(path_list)>=0:        
    s3_path_dict = {}
    s3_path_dict['fileLocations'] = path_list
    with open('manifest_sp_locations.json', 'w',encoding = 'UTF-8') as outfile:
        json.dump(s3_path_dict, outfile)
    #print("SP Log Info : --")
    #print(path_list)
    with open('manifest_sp_locations.json', 'w',encoding = 'UTF-8') as outfile:
        json.dump(s3_path_dict,outfile)
    with open('manifest_sp_locations.json', 'rb') as data:
        s3.upload_fileobj(data, 'odp-us-prod-raw', 'servicesuite/ODP/manifest/manifest_sp_locations.json')

response = s3.get_object(
    Bucket='odp-us-prod-raw',
    Key='servicesuite/ODP/manifest/manifest_sp_order.json'
)
file_content = response['Body'].read().decode('utf-8')
json_content = json.loads(file_content)
sp_order_list = json_content['fileLocations']

if len(sp_order_list)==0 and len(path_list)==0:
    print('FAILURE')
else:
    print('SUCCESS')