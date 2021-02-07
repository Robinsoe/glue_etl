# glue_etl script

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# Glue/Spark context set up
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'glue_db', 's3_bucket', 'svr', 'db', 'sch', 'tbl', 'partition_by', 'job-bookmark-option'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

# Table reference data
glue_db = args['glue_db']
s3_bucket = args['s3_bucket']
svr = args['svr']
db = args['db']
sch = args['sch']
tbl = args['tbl']
partition_by = args['partition_by']
bookmark = args['job-bookmark-option']
db_adj = db.replace("-", "_")
table_name = db_adj + '_'+ sch +'_' + tbl
s3_path ='s3://' + s3_bucket + '/' + svr + '/' + db + '/' + sch + '/' + tbl
prefix = svr + '/' + db + '/' + sch + '/' + tbl

# Get glue catalog table data
glue = boto3.client('glue', region_name='us-west-2')
table = glue.get_table(DatabaseName=glue_db, Name=table_name).get('Table')
tableStorageDescriptor = table.get('StorageDescriptor')
tableColumns = tableStorageDescriptor.get('Columns')

# Get mapping
mapping = []
for column in tableColumns:
    mapping.append((column['Name'], column['Type'], column['Name'], column['Type']))

# Delete prior runs s3 file
if bookmark == 'job-bookmark-disable':
    s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
    bucket = s3.Bucket(s3_bucket)
    objs = bucket.objects.filter(Prefix=prefix)
    for obj in objs:
        print(obj.key)
        obj.delete()

# Run glue job
ctx = tbl
job.init(args['JOB_NAME'], args)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database=glue_db, table_name=table_name, transformation_ctx="datasource0" + ctx)
applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=mapping, transformation_ctx="applymapping1" + ctx)
resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_struct", transformation_ctx="resolvechoice2" + ctx)
dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3" + ctx)
datasink4 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields3, connection_type="s3", connection_options={"path": s3_path, "partitionKeys": [partition_by]}, format="parquet", transformation_ctx="datasink4" + ctx)
job.commit()


