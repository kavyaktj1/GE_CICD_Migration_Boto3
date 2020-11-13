#from botocore.vendored import requests
import requests
import requests_oauthlib
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
import os
import json
import sys
import boto3

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'





def lambda_handler(event, context):
    report_location=event.get('REPORT_LOCATION')
    body='''
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ext="http://spotfire.tibco.com/ws/2015/08/externalScheduledUpdate.xsd">
    <soapenv:Header/>
    <soapenv:Body>
    <ext:loadAnalysis>
    <!--Optional:-->
    <updateAnalysis>
    <!--Optional:-->
    <path>{report_location}</path>
    <!--Optional:-->
    <clientUpdate>Manual</clientUpdate>
    <keepAliveMinutes>10</keepAliveMinutes>
    <!--Optional:-->
    </updateAnalysis>
    </ext:loadAnalysis>
    </soapenv:Body>
    </soapenv:Envelope>
    '''.format(report_location=report_location)
    secret = get_spotfire_secrets()
    client_id = secret.get('client_id')
    client_secret = secret.get('client_secret')
    token_url = secret.get('token_url')
    url = secret.get('url')
    auth = HTTPBasicAuth(client_id, client_secret)
    client = BackendApplicationClient(client_id=client_id)
    
    oauth = OAuth2Session(client=client)
    
    scope='api.soap.update-analysis-service'
    token = oauth_fetch_token(token_url,client_id,client_secret,scope,oauth)
    string_token=token.get('access_token')

    headers = {
    'authorization': "Bearer " + string_token,
    'content-type': "application/xml",
    }

    r = requests.post(url, headers=headers, data=body)
    print(r.status_code)
    if r.status_code != 200:
        print("HTTP response is not successful")
        print ("Response :",r.text)
        sys.exit(1)

def get_spotfire_secrets():
    client = boto3.client("secretsmanager", region_name='us-east-1')
    get_secret_value_response = client.get_secret_value(SecretId='us-prod-spotfire-secrets')
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    return secret



def oauth_fetch_token(token_url,client_id,client_secret,scope,oauth):
    try :
       token = oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret,scope=scope)
       return token
    except Exception as e:
        print ('Error :',str(e))
        sys.exit(1)

