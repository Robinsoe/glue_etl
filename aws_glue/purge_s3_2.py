

import boto3
s3_bucket = 'crcdal-well-data'
prefix = 'source-glue'
s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3.Bucket(s3_bucket)
objs = bucket.objects.filter(Prefix=prefix)
for obj in objs:
    print(obj.key)
    obj.delete()