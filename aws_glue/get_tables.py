
import boto3
from datetime import date

bucket = 'xspoc-high-res-glue_eggs'
s3_db = "xspoc_high_res"
server = "ckcwsqlb"
db_list = ["ekxspoc","bvxspoc","bkxspoc","lhxspoc","lgbxspoc","huxspoc","kfxspoc","s3xspoc"]
tbl_list = ["tbldatahistory","tblcarddata","tblxdiagflags","tblxdiagresults","tblxdiagrodresults"]
date_file = date.today().strftime("%Y-%m-%d")

client = boto3.client('glue', region_name='us-west-2')
responseGetDatabases = client.get_databases()
databaseList = responseGetDatabases['DatabaseList']
responseGetTables = client.get_tables( DatabaseName = s3_db)
tableList = responseGetTables['TableList']

tbl_file = []
for tbl in tbl_list:
    for db in db_list:
         mapping = []
         for tableDict in tableList:
              if tableDict['Name'] == db + '_dbo_' + tbl:
                   columns = tableDict['StorageDescriptor']['Columns']
                   for column in columns:
                        mapping.append((column['Name'],column['Type'],column['Name'],column['Type']))
         tbl_file.append((db + '_dbo_' + tbl
                          , 's3://' + bucket + '/' + tbl + '/' + server + '/' + db + '/' + date_file
                          , tbl + '/' + server + '/' + db + '/' + date_file
                          , mapping
                          , tbl))

s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3.Bucket(bucket)

for tbl, file, prefix, mapping, ctx in tbl_file:
    print(tbl)
    print(file)
    print(mapping)
    print(ctx)

    # objs = bucket.objects.filter(Prefix=prefix)
    # for obj in objs:
    #     print(obj.key)
    #     obj.delete()
    #
    # s3_client = boto3.client('s3', region_name='us-west-2', verify=False)
    # objs = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    # for obj in objs['Contents']:
    #     print(obj['Key'])
    #     s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
    #
    # s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
    # bucket = s3.Bucket(bucket)
    # objs = bucket.objects.filter(Prefix=prefix)
    # for obj in objs:
    #     print(obj.key)
    #     obj.delete()




import boto3
prefix = 'S-Rod Files'
bucket = 'crc-srod'
s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3.Bucket(bucket)
objs = bucket.objects.filter(Prefix=prefix)
for obj in objs:
    print(obj.key)