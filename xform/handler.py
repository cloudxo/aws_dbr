import os
import re
import subprocess
import uuid
import boto3

DEST_BUCKET = 'koblas-tubular-test'

def handler(event, context):
    pat = re.compile(r'.+-aws-billing-detailed-line-items-with-resources-and-tags-(\d{4}-\d{2}).csv.zip')

    for record in event['Records']:
        # Find input/output buckets and key names
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if not pat.match(key):
            print "Skipping update %s" % key
            continue

        src = 's3://%s%s' % (bucket, key)
        dst = 's3://%s/dbr%s' % (DEST_BUCKET, key.replace('.csv.zip', '.avro'))

        subprocess.check_call(['./bill_lambda', src, dest])
