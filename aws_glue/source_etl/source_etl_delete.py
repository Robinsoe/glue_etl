# source_etl_delete

import etl_setup
import etl_functions
import os
import pandas as pd

if __name__ == '__main__':

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
                LIMIT 30
                """
    conn_string = ("Driver={PostgreSQL Unicode};Server=bda-aurora-postgresql-cluster.cluster-ro-c2it5k2mwyhf.us-west-2.rds.amazonaws.com;Database=bda-aurora;UID=svc_prod_ops;PWD=svc_prod_ops; Trusted_Connection=yes")
    df = etl_functions.getSQLData(sql_string, conn_string, plk_data_dir, data_file, updateLocal=ansr)
    s3_bucket_name = 'sources-glue'

    # crcdal inputs
    # data = {'servername':['aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora','aurora'],
    #         'databasename':['bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora','bda-aurora'],
    #         'schemaname': ['bda','bda','bda','bda','bda','prod_ops','prod_ops','crc','crc','ds_usoxybip','calgem','ds_ekpspp'],
    #         'tablename':['merged_all_picks2','well_perf_hist','well_surveys','v_well_info_general_detail_with_crc_fields','v_vnreserves_static_custom_fields','well_note_hist','well_operations_summary_hist','bi_monthly_volumes','bi_well','crcplan_mer_vn_outlook','allwells','site_crc_pressures'],
    #         'migrationtype': ['','','','','','','','','','','','',],
    #         'groupno': ['main','main','main','main','main','main','main','main','main','main','main','main'],
    #         }
    # df = pd.DataFrame(data)
    # s3_bucket_name = 'crcdal-glue'

    # Connection List
    con_list = etl_setup.get_con_list()

    # Delete Everything
    print('Deleting workflows')
    for index, row in df.iterrows():
        svr = row['servername']
        db = row['databasename']
        sch = row['schemaname']
        tbl = row['tablename']
        glue_db_name = f'{svr.lower()}_{db.lower()}'
        workflow_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_wf'
        jdbc_crawler_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_crawler_jdbc'
        s3_crawler_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_crawler_s3'
        db_adj = db.replace("-", "_")
        table_name = f'{db_adj.lower()}_{sch.lower()}_{tbl.lower()}'
        s3_table_name = f'{tbl.lower()}'
        job_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_job'
        trigger_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_trigger'
        etl_functions.delete_wf_all(glue_db_name, table_name, s3_table_name, jdbc_crawler_name, s3_crawler_name, job_name, trigger_name, workflow_name)
