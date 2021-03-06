# source_etl

import etl_setup
from etl_functions import ETLFunctions
import os
import pandas as pd

if __name__ == '__main__':

    etl_functions = ETLFunctions()
    etl_functions.tag = 'data_engineering'
    # etl_functions.tag = 'data_science'

    # Query Utility Table
    username = os.getlogin()
    plk_data_dir = 'C:/Users/' + username + '/'
    ansr = True
    data_file = 'utility_table.pkl'
    sql_string = """
                SELECT *
                FROM (
                    SELECT (CASE WHEN servername = 'ccslpsq01' THEN 'CKCWSQLB' ELSE servername END) servername
                        , databasename, schemaname, tablename, migrationtype, groupno
                    FROM utility.data_sources_v
                ) t
                LIMIT 5
                """
    conn_string = ("Driver={PostgreSQL Unicode};Server=bda-aurora-postgresql-cluster.cluster-ro-c2it5k2mwyhf.us-west-2.rds.amazonaws.com;Database=bda-aurora;UID=svc_prod_ops;PWD=svc_prod_ops; Trusted_Connection=yes")
    df = etl_functions.getSQLData(sql_string, conn_string, plk_data_dir, data_file, updateLocal=ansr)
    s3_bucket_name = 'sources-glue'

    # create s3 bucket
    s3_bucket_list = etl_functions.s3_bucket_list()
    if s3_bucket_name not in s3_bucket_list:
        etl_functions.create_s3_bucket(s3_bucket_name)
        print('S3 bucket created')

    # upload glue job scripts to s3 bucket
    script_path = f'./'
    etl_functions.upload_glue_scripts_to_s3_bucket(s3_bucket_name, script_path)
    print('Upload scripts to s3')

    # Glue script name
    script_name = 'glue_script_source_to_catalog'

    # Connection List
    con_list = etl_setup.get_con_list()

    # Loop through utility table
    for index, row in df.iterrows():
        svr = row['servername']
        db = row['databasename']
        sch = row['schemaname']
        tbl = row['tablename']
        type = row['migrationtype']
        group = row['groupno']
        print(f'ETL: {db.lower()}_{sch.lower()}_{tbl.lower()}')

        etl_functions.create_source_to_glue_cat_etl(svr, db, sch, tbl, type, group, con_list, s3_bucket_name, script_name)
        # etl_functions.delete_source_to_glue_cat_etl(svr, db, sch, tbl)



