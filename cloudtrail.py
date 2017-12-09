from __future__ import print_function
import json
import os
import urllib
import datetime
from io import BytesIO
from gzip import GzipFile
import boto3

S3 = boto3.client('s3')
DYNAMO = boto3.resource('dynamodb')
#DYNAMO = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
TABLE = DYNAMO.Table(os.environ.get('DYNAMODB_TABLE', None))

def get_from_s3(bucket, key):
    """This function gets files from S3.
    Cloudtrail says "New $key in S3 bucket, then we have to go fetch and gunzip it"
    """
    try:
        response = S3.get_object(Bucket=bucket, Key=key)
        bytestream = BytesIO(response['Body'].read())
        contents = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
        return contents
    except Exception as e:
        print(e)
        raise

def save_cloudtrail_event(record):
    """Now that we have a record, stuff it into the storage engine
    """
    record['id'] = record['requestID']
    TABLE.put_item(
        Item=record
    )
#        es.index(index=indexname,
#                 doc_type='record',
#                 id=record['requestID'],
#                 body=record)

def insert(event, context):
    """Track the count of what we could and could not insert, and call the func to insert
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    cloudtrail_log = get_from_s3(bucket, key)
    records = json.loads(cloudtrail_log)
    counter_good = []
    counter_bad = []
    for record in records['Records']:
        try:
            save_cloudtrail_event(record)
            counter_good.append('a')
        except Exception as e:
            counter_bad.append('a')
            print("Failed to insert into storage. Error: %s" % (e))
            print(json.dumps(record))
    client = boto3.client('cloudwatch', region_name=os.environ.get('AWS_DEFAULT_REGION'))
    
    client.put_metric_data(Namespace="cloudtrail/cloudtrail_indexer",
                           MetricData=[
                               {'MetricName': 'cloudtrail_indexing',
                                'Dimensions': [
                                   {'Name': 'docs_indexed',
                                    'Value': 'success'
                                   }],
                                'Value': len(counter_good)},
                               {'MetricName': 'cloudtrail_indexing',
                                'Dimensions': [
                                   {'Name': 'docs_indexed',
                                    'Value': 'failed'
                                   }],
                                'Value': len(counter_bad)}
                                ])
