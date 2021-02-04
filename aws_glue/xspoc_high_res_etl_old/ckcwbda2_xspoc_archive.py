##ckcwbda2_xspoc_archive

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import date
import boto3

# Table reference data
s3_bucket = 'xspoc-high-res-glue_eggs'
s3_db = "xspoc_high_res"
server = "ckcwbda2"
db_list = ['EKXSPOC','BVXSPOC','BKXSPOC','LHXSPOC','LGBXSPOC','HUXSPOC','KFXSPOC','S3XSPOC']
tbl_list = [
    'tblXDiagOutputs',
    'tblXDiagResultsLast',
    'tblXDiagScores',
    'tblXDiagResults',
    'tblXDiagRodResults',
    'tblXDiagFlags',
    'tblDataHistory',
    'tblDataHistoryArchive',
    'tblCardData'
    ]

#Glue/Spark context set up
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

# Get glue catalog table data
glue = boto3.client('glue', region_name='us-west-2')
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

# Build meta data refernce for each glue job
date_file = date.today().strftime("%Y-%m-%d")
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
        tbl_file.append(('bda_ckcwsqlb_' + db + '_dbo_dbo_' + tbl
                , 's3://' + s3_bucket + '/' + tbl + '/' + server + '/' + db + '/' + date_file
                , tbl + '/' + server + '/' + db + '/' + date_file
                , mapping
                , tbl))

# Run glue jobs
s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
bucket = s3.Bucket(s3_bucket)
sns = boto3.client('sns', region_name='us-west-2')
for tbl, file, prefix, mapping, ctx in tbl_file:

    # Delete prior runs s3 file
    objs = bucket.objects.filter(Prefix=prefix)
    for obj in objs:
        print(obj.key)
        obj.delete()

    # Run job
    try:
        job.init(args['JOB_NAME'], args)
        datasource0 = glueContext.create_dynamic_frame.from_catalog(database = s3_db, table_name = tbl, transformation_ctx = "datasource0" + ctx, additional_options ={'hashfield': 'NodeID', 'hashpartitions': '10'}))
        applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = mapping, transformation_ctx = "applymapping1" + ctx)
        resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2" + ctx)
        dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3" + ctx)
        datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": file }, format = "parquet", transformation_ctx = "datasink4" + ctx)
        job.commit()
    except:
        print('glue_etl failed: ' + tbl)
        logger.info('glue_etl failed: ' + tbl)
        sns.publish(
            Message = 'glue_etl failed: ' + tbl,
            PhoneNumber = '+16614483439')
        continue



