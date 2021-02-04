##ckcwsqlb_xspoc

import sys
from datetime import date
import boto3

s3_bucket = 'xspoc-glue_eggs'
s3_db = "xspoc"
server = "ckcwsqlb"
db_list = ["ekxspoc","bvxspoc","bkxspoc","lhxspoc","lgbxspoc","huxspoc","kfxspoc","s3xspoc"]
tbl_list = ['tblNodeMaster']

date_file = date.today().strftime("%Y-%m-%d")

glue = boto3.client('glue_eggs', region_name='us-west-2')
tableListName = []
tableListColumns = []
next_token = ""
while True:
    responseGetTables = glue.get_tables(DatabaseName=s3_db, NextToken=next_token)
    for table in responseGetTables.get('TableList'):
        tableListName.append(table.get('Name'))
        tableListStorageDescriptor = table.get('StorageDescriptor')
        tableListColumns.append(tableListStorageDescriptor.get('Columns'))
    next_token = responseGetTables.get('NextToken')
    if next_token is None:
        break

s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3.Bucket(s3_bucket)

sns = boto3.client('sns', region_name='us-west-2')

server = server.lower()
tbl_file = []
for tbl in tbl_list:
    tbl = tbl.lower()
    for db in db_list:
        db = db.lower()
        mapping = []
        for i in range(len(tableListName)):
             if tableListName[i] == db + '_dbo_' + tbl:
                columns = tableListColumns[i]
                for column in columns:
                    mapping.append((column['Name'],column['Type'],column['Name'],column['Type']))
        tbl_file.append((db + '_dbo_' + tbl
                , 's3://' + s3_bucket + '/' + tbl + '/' + server + '/' + db
                , tbl + '/' + server + '/' + db
                , mapping
                , tbl))

for tbl, file, prefix, mapping, ctx in tbl_file:

    print(tbl)
    print(file)
    print(prefix)
    print(mapping)
    print(ctx)
    print('\n')
