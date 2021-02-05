# glue_etl script

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# crcdal inputs
# data = {'servername':['aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora'],
#         'databasename':['bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora'],
#         'schemaname': ['bda','bda','bda','bda','bda','prod_ops','prod_ops','crc','crc','ds_usoxybip','calgem','ds_ekpspp'],
#         'tablename':['merged_all_picks2','well_perf_hist','well_surveys','v_well_info_general_detail_with_crc_fields','v_vnreserves_static_custom_fields','well_note_hist','well_operations_summary_hist','bi_monthly_volumes','bi_well','crcplan_mer_vn_outlook','allwells','site_crc_pressures'],
#         'migrationtype': ['','','','','','','','','','','','',],
#         'groupno': ['main','main','main','main','main','main','main','main','main','main','main','main'],
#         'apivar': ['well_uwi','api_no14','api_no14','api_no14','api14','api_no14','api_no14','api_no14','api_no10', 'api_number', 'api', 'pid12']
#         }
# df = pd.DataFrame(data)

data = {'servername': ['aurora'],
        'databasename': ['bda-aurora'],
        'schemaname': ['bda'],
        'tablename': ['v_well_info_general_detail_with_crc_fields'],
        'migrationtype': [''],
        'groupno': ['main'],
        'apivar': ['api_no14']
        }
df = pd.DataFrame(data)

s3_bucket = 'crcdal-well-data'
s3_bucket_etc = 'source-glue'
# Loop through utility table
for index, row in df.iterrows():

    svr = row['servername']
    db = row['databasename']
    sch = row['schemaname']
    tbl = row['tablename']
    api = row['apivar']

    glue_db_name = f'{svr.lower()}_{db.lower()}'
    table_name = f'{tbl.lower()}'
    s3_path =f's3://{s3_bucket}/{s3_bucket_etc}/{sch}/{tbl}'
    prefix = f'{s3_bucket_etc}/{sch}/{tbl}'

    # Delete prior runs s3 file
    s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
    bucket = s3.Bucket(s3_bucket)
    objs = bucket.objects.filter(Prefix=prefix)
    for obj in objs:
        print(obj.key)
        obj.delete()

    # Get glue catalog table data
    glue = boto3.client('glue', region_name='us-west-2')
    table = glue.get_table(DatabaseName=glue_db_name, Name=table_name).get('Table')
    tableStorageDescriptor = table.get('StorageDescriptor')
    tableColumns = tableStorageDescriptor.get('Columns')

    # Get mapping
    mapping = []
    for column in tableColumns:
        mapping.append((column['Name'], column['Type'], column['Name'], column['Type']))

    job.init(args['JOB_NAME'], args)
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = glue_db_name, table_name = table_name, transformation_ctx = "datasource0", additional_options ={'hashfield': api})
    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = mapping, transformation_ctx = "applymapping1")
    resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
    dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
    datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": s3_path, "partitionKeys": [api]}, format = "parquet", transformation_ctx = "datasink4")
    job.commit()


