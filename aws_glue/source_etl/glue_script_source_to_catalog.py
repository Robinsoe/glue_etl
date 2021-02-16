# glue_etl script

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# Glue/Spark context set up
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'glue_db', 'table_name', 's3_path', 'partition_by', 'bookmark'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

# Table reference data
glue_db = args['glue_db']
table_name = args['table_name']
s3_path = args['s3_path']
partition_by = args['partition_by']
bookmark = args['bookmark']

# Get glue catalog table data for mapping
glue = boto3.client('glue', region_name='us-west-2')
table = glue.get_table(DatabaseName=glue_db, Name=table_name).get('Table')
tableStorageDescriptor = table.get('StorageDescriptor')
tableColumns = tableStorageDescriptor.get('Columns')
mapping = []
for column in tableColumns:
    mapping.append((column['Name'], column['Type'], column['Name'], column['Type']))

# Delete prior runs s3 file
if bookmark == 'N':
    glueContext.purge_s3_path(s3_path)

# Set connection options
if partition_by == 'None':
    connection_options = {"path": s3_path}
else:
    connection_options = {"path": s3_path, "partitionKeys": [partition_by]}

# Run glue job
ctx = table_name
job.init(args['JOB_NAME'], args)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database=glue_db, table_name=table_name, transformation_ctx="datasource0" + ctx)
applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=mapping, transformation_ctx="applymapping1" + ctx)
resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_struct", transformation_ctx="resolvechoice2" + ctx)
dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3" + ctx)
datasink4 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields3, connection_type="s3", connection_options=connection_options, format="glueparquet", transformation_ctx="datasink4" + ctx)
job.commit()


