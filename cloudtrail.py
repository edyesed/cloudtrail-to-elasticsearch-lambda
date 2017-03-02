from __future__ import print_function
import sys
sys.path.append("./vendored")
import json
import boto3
import os
import elasticsearch
import urllib
import gzip
import datetime
from io import BytesIO
from gzip import GzipFile
from requests_aws4auth import AWS4Auth
#

ES_HOST = os.environ.get('ELASTICSEARCH_URL',None)

s3 = boto3.client('s3')

def get_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("response from s3 is " ) 
        bytestream = BytesIO(response['Body'].read())
        contents = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        print("CONTENTS : " + contents)
        return contents
    except Exception as e:
        print(e)
        raise

def insert_into_es(record):
    try:
        cred = boto3.session.Session().get_credentials()
        awsauth = AWS4Auth(cred.access_key,
                           cred.secret_key,
                           os.environ.get('AWS_DEFAULT_REGION'),
                           'es',
                           session_token=cred.token)
        es = elasticsearch.Elasticsearch(
            hosts=[ES_HOST],
            connection_class=elasticsearch.RequestsHttpConnection,
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True)
        es.info()
    except Exception as e:
        print("FAILED TO TALK TO AMAZON ES, because %s" % (e))
        raise(e)
    try:
        indexname = datetime.datetime.now().strftime("cloudtrail-%Y-%m-%d")
        es.index(index=indexname,
                 doc_type='record',
                 id=record['requestID'],
                 body=record)
    except Exception as e:
        print("FAILED TO INSERT RECORD IN ES, because %s" % (e))
        raise(e)

def insert(event, context):
    # Get the object from the event and show its content type
    print("EVENT")
    print(json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    # ok
    cloudtrail_log = get_from_s3(bucket, key)
    records = json.loads(cloudtrail_log)
    counter_good = []
    counter_bad = []
    for record in records['Records']:
        try:
            insert_into_es(record)
            counter_good.append('a')
        except Exception as e:
            counter_bad.append('a')
            print("Failed to insert into ES. %s" % (e))
            print(json.dumps(record))
    client = boto3.client('cloudwatch', region_name=os.environ.get('AWS_DEFAULT_REGION'))
    
    client.put_metric_data(Namespace="cloudtrail/es_indexer",
                           MetricData=[
                               {'MetricName': 'es_cloudtrail_indexing',
                                'Dimensions': [
                                   {'Name': 'docs_indexed',
                                    'Value': 'success'
                                   }],
                                'Value': len(counter_good)},
                               {'MetricName': 'es_cloudtrail_indexing',
                                'Dimensions': [
                                   {'Name': 'docs_indexed',
                                    'Value': 'failed'
                                   }],
                                'Value': len(counter_bad)}
                                ])
