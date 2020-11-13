import json
import boto3
import re
from datetime import datetime


def lambda_handler(event, context):
    result = str(event['detail']['status']) 
    timestamp=str( datetime.now() )
    client = boto3.client('s3')
    arn=str(event['detail']['stateMachineArn'])
    names=arn.split(":")
    sfname=names[len(names)-1]
    #result=re.findall(r'\'(.*?)\'',result)[0]
    if(sfname.find("\'")!=-1):
        k=sfname.index("\'")
        sfname=sfname[:k]
    keyname='odp/daily/'+sfname+'_'+result+'.txt'
    client.put_object(Body=timestamp, Bucket='odp-us-prod-orchestration', Key=keyname)
    return {
        
        'result':keyname,
        'statusCode': 200,
        'body': json.dumps(timestamp)
    }