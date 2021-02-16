
import boto3
s3_bucket = 'crcdal-glue'
prefix = 'aurora'
s3resource = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3resource.Bucket(s3_bucket)
# bucket.objects.all().delete()
# bucket.objects.filter(Prefix=prefix).delete()
objs = bucket.objects.filter(Prefix=prefix)
for obj in objs:
    print(obj.key)
    obj.delete()


# import awswrangler as wr
# s3_bucket = 'crcdal-glue'
# prefix = 'aurora'
# wr.s3.delete_objects(f's3://{s3_bucket}/{prefix}')