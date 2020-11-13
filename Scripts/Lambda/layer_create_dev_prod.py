# This script will take the updated config and zipped code as input and will create/update the lambda functions.

import sys
import boto3
import time
import json
import re
import pandas
from zipfile import ZipFile
from time import sleep
import shutil
import os


client = boto3.client('lambda',region_name='us-east-1')

df = pandas.read_csv('lambda_layers_dev_prod.csv')
df = df.dropna(axis = 0, how = 'any')
path_prefix = sys.argv[1]

size = len(df)
for index in range(size):
    layer_name = df['Layer'][index]
    file = open(path_prefix+'/tempdir_odp/ODP/JobConfig/PROD/lambda/Layers/'+layer_name+'.zip','rb') # Gets the Zipped code from Git.
    zip_code = file.read()
    response = client.publish_layer_version(
        LayerName=layer_name,
        Content={
            'ZipFile': zip_code
        },
        CompatibleRuntimes=[
            'python3.6',
            'python3.7',
        ]
    )
    print('Lambda Layer : ',layer_name,' created')