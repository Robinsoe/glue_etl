# source_etl

import etl_setup
from etl_functions import ETLFunctions
import os
import pandas as pd

if __name__ == '__main__':

    etl_functions = ETLFunctions()
    # etl_functions.tag = 'data_engineering'
    etl_functions.tag = 'data_science'

    # crcdal inputs
    data = {'servername':['aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora'],
            'databasename':['bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora'],
            'schemaname': ['bda','bda','bda','bda','bda','prod_ops','prod_ops','crc','crc','ds_usoxybip','calgem','ds_ekpspp'],
            'tablename':['merged_all_picks2','well_perf_hist','well_surveys','v_well_info_general_detail_with_crc_fields','v_vnreserves_static_custom_fields','well_note_hist','well_operations_summary_hist','bi_monthly_volumes','bi_well','crcplan_mer_vn_outlook','allwells','site_crc_pressures'],
            'migrationtype': ['','','','','','','','','','','','',],
            'groupno': ['main','month','main','month','month','month','month','main','month','month','month','month'],
            'apivar': ['well_uwi', 'api_no14', 'api_no14', 'api_no14', 'api14', 'api_no14', 'api_no14', 'api_no14', 'api_no10', 'api_number', 'api', 'pid12'],
            'bookmark': ['Y','N','Y','N','N','N','N','Y','N','N','N','N'],
            }
    df = pd.DataFrame(data)

    # crcdal inputs
    # data = {'servername': ['aurora'],
    #         'databasename': ['bda-aurora'],
    #         'schemaname': ['bda'],
    #         'tablename': ['v_well_info_general_detail_with_crc_fields'],
    #         'migrationtype': [''],
    #         'groupno': ['main'],
    #         'apivar': ['api_no14'],
    #         'bookmark': ['N']
    #         }
    # df = pd.DataFrame(data)

    s3_bucket_name = 'crcdal-glue'
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
        partition_by = row['apivar']
        bookmark = row['bookmark']
        print(f'ETL: {db.lower()}_{sch.lower()}_{tbl.lower()}')

        etl_functions.create_source_to_glue_cat_etl(svr, db, sch, tbl, type, group, con_list, s3_bucket_name, script_name, partition_by, bookmark)
        # etl_functions.delete_source_to_glue_cat_etl(svr, db, sch, tbl, partition_by)



