
import boto3
s3resource = boto3.resource('s3', region_name='us-west-2', verify=False)
name = 'crcdal-glue'
buckettoempty = s3resource.Bucket(name)
buckettoempty.objects.all().delete()
print(f'{name} S3 bucket emptied')


# import boto3
# s3_bucket = 'crcdal-glue'
# prefix = 'aurora'
# s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
# bucket = s3.Bucket(s3_bucket)
# objs = bucket.objects.filter(Prefix=prefix)
# for obj in objs:
#     print(obj.key)
#     obj.delete()