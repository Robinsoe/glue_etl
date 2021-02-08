# source_etl

import etl_setup
from etl_functions import ETLFunctions
import os
import pandas as pd

if __name__ == '__main__':

    # instantiate etl
    s3_bucket_name = 'sources-glue'
    etl_functions = ETLFunctions(s3_bucket_name)
    etl_functions.tag = 'data_engineering'
    # etl_functions.tag = 'data_science'
    etl_functions.script_name = 'glue_script_source_to_catalog'

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

    # Loop through utility table
    for index, row in df.iterrows():
        print('ETL: ' + row['tablename'].lower())

        etl_functions.create_source_to_glue_cat_etl(
            svr=row['servername'],
            db = row['databasename'],
            sch = row['schemaname'],
            tbl = row['tablename'],
            type = row['migrationtype'],
            group = row['groupno'],
            # partition_by = row['apivar'],
            # bookmark = row['bookmark'],
            start_wf = False
        )
        # etl_functions.delete_source_to_glue_cat_etl(
        #     svr=row['servername'],
        #     db = row['databasename'],
        #     sch = row['schemaname'],
        #     tbl = row['tablename'],
        #     # partition_by=row['apivar']
        # )



