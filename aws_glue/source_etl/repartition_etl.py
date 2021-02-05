# source_etl

# import etl_setup
from etl_functions import ETLFunctions
import os
import pandas as pd

if __name__ == '__main__':

    etl_functions = ETLFunctions()
    # etl_functions.tag = 'data_engineering'
    etl_functions.tag = 'data_science'

    # crcdal jdbc sources inputs
    data = {'servername':['aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora'],
            'databasename':['bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora'],
            'schemaname': ['bda','bda','bda','bda','bda','prod_ops','prod_ops','crc','crc','ds_usoxybip','calgem','ds_ekpspp'],
            'tablename':['merged_all_picks2','well_perf_hist','well_surveys','v_well_info_general_detail_with_crc_fields','v_vnreserves_static_custom_fields','well_note_hist','well_operations_summary_hist','bi_monthly_volumes','bi_well','crcplan_mer_vn_outlook','allwells','site_crc_pressures'],
            'migrationtype': ['','','','','','','','','','','','',],
            'groupno': ['main','main','main','main','main','main','main','main','main','main','main','main'],
            'apivar': ['well_uwi', 'api_no14', 'api_no14', 'api_no14', 'api14', 'api_no14', 'api_no14', 'api_no14', 'api_no10', 'api_number', 'api', 'pid12']
            }
    df = pd.DataFrame(data)

    # crcdal jdbc sources inputs
    # data = {'servername': ['aurora'],
    #         'databasename': ['bda-aurora'],
    #         'schemaname': ['bda'],
    #         'tablename': ['v_well_info_general_detail_with_crc_fields'],
    #         'migrationtype': [''],
    #         'groupno': ['main'],
    #         'apivar': ['api_no14']
    #         }
    # df = pd.DataFrame(data)

    target_glue_db = 'crcdalv2'
    target_s3_bucket_name = 'crcdal-well-data'
    target_s3_bucket_etc = 'source-glue'

    # create s3 bucket
    s3_bucket_list = etl_functions.s3_bucket_list()
    if target_s3_bucket_name not in s3_bucket_list:
        etl_functions.create_s3_bucket(target_s3_bucket_name)
        print('S3 bucket created')

    # upload glue job scripts to s3 bucket
    script_path = f'./'
    etl_functions.upload_glue_scripts_to_s3_bucket(target_s3_bucket_name, script_path)
    print('Upload scripts to s3')

    # Loop through utility table
    for index, row in df.iterrows():
        svr = row['servername']
        db = row['databasename']
        sch = row['schemaname']
        tbl = row['tablename']
        type = row['migrationtype']
        group = row['groupno']
        api = row['apivar']
        print(f'Repartition ETL: {db.lower()}_{sch.lower()}_{tbl.lower()}')

        etl_functions.create_glue_repartition_etl_all(svr, db, sch, tbl, type, group, api, target_s3_bucket_name, target_s3_bucket_etc, target_glue_db)
        # etl_functions.delete_glue_repartition_etl_all(svr, db, sch, tbl, target_glue_db)
    



