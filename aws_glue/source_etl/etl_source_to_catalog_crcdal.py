# source_etl
from etl_functions import ETLFunctions
import pandas as pd

if __name__ == '__main__':

    # instantiate etl
    s3_bucket_name = 'crcdal-glue'
    etl_functions = ETLFunctions(s3_bucket_name)
    # etl_functions.tag = 'data_engineering'
    etl_functions.tag = 'data_science'
    etl_functions.script_name = 'glue_script_source_to_catalog'

    # target_glue_db = 'crcdalv2'
    # target_s3_bucket_name = 'crcdal-well-data'
    # target_s3_bucket_etc = 'source-glue'

    # crcdal inputs
    data = {'servername':['aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora'],
            'databasename':['bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora'],
            'schemaname': ['bda','bda','bda','bda','bda','prod_ops','prod_ops','bda','crc','ds_usoxybip','calgem','ds_ekpspp'],
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
    #         'groupno': ['month'],
    #         'apivar': ['api_no14'],
    #         'bookmark': ['N']
    #         }
    # df = pd.DataFrame(data)

    # crcdal inputs
    # data = {'servername': ['aurora'],
    #         'databasename': ['bda-aurora'],
    #         'schemaname': ['bda'],
    #         'tablename': ['bi_monthly_volumes'],
    #         'migrationtype': [''],
    #         'groupno': ['main'],
    #         'apivar': ['api_no14'],
    #         'bookmark': ['Y']
    #         }
    # df = pd.DataFrame(data)

    # Loop through utility table
    for index, row in df.iterrows():
        print('ETL: ' + row['tablename'].lower())

        etl_functions.create_source_to_glue_cat_etl(
            svr = row['servername'],
            db = row['databasename'],
            sch = row['schemaname'],
            tbl = row['tablename'],
            type = row['migrationtype'],
            group = row['groupno'],
            partition_by = row['apivar'],
            bookmark = row['bookmark']
        )
        # etl_functions.delete_source_to_glue_cat_etl(
        #     svr = row['servername'],
        #     db = row['databasename'],
        #     sch = row['schemaname'],
        #     tbl = row['tablename'],
        #     partition_by=row['apivar']
        # )



