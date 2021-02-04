import boto3
from datetime import date

bucket_name = 'xspoc-high-res-glue_eggs'
# old_prefix = 'tblcarddata'
# new_prefix = 'copy_tblcarddata'
old_prefix = 'copy_tblcarddata'
new_prefix = 'tblcarddata/copy_tblcarddata'

s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3.Bucket(bucket_name)
objs = bucket.objects.filter(Prefix=old_prefix)

for obj in objs:
    print(obj.key)
    old_source = {'Bucket': bucket_name, 'Key': obj.key}
    new_key = obj.key.replace(old_prefix, new_prefix, 1)
    new_obj = bucket.Object(new_key)
    new_obj.copy(old_source)


