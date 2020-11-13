import json
import boto3
s3 = boto3.resource('s3')
def lambda_handler(event, context):
    bucket = s3.Bucket('odp-us-prod-orchestration')
    for obj in bucket.objects.filter(Prefix='odp/daily'):
        s3.Object(bucket.name,obj.key).delete()
    for obj in bucket.objects.filter(Prefix='cronacle/daily/'):
        s3.Object(bucket.name,obj.key).delete()    
    return {
        'statusCode': 200,
        'body': json.dumps('Objects deleted!')
    }
