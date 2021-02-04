# source_etl

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

    # list the existing glue objects
    s3_bucket_list = etl_functions.s3_bucket_list()
    glue_db_list = etl_functions.glue_db_list()
    glue_con_list = etl_functions.glue_con_list()
    glue_crawler_list = etl_functions.glue_crawler_list()
    glue_workflow_list = etl_functions.glue_wf_list()
    glue_trigger_list = etl_functions.glue_trigger_list()
    glue_job_list = etl_functions.glue_jobs_list()

    # create s3 bucket
    if s3_bucket_name not in s3_bucket_list:
        etl_functions.create_s3_bucket(s3_bucket_name)
        print('S3 bucket created')

    # upload glue job scripts to s3 bucket
    script_path = f'./'
    etl_functions.upload_glue_scripts_to_s3_bucket(s3_bucket_name, script_path)
    print('Upload scripts to s3')

    # Loop through utility table
    for index, row in df.iterrows():
        svr = row['servername']
        db = row['databasename']
        sch = row['schemaname']
        tbl = row['tablename']
        type = row['migrationtype']
        group = row['groupno']
        print(f'Create ETL: {db.lower()}_{sch.lower()}_{tbl.lower()}')

        # create glue connections jdbc
        glue_con_jdbc_name = f'{svr.lower()}'
        con = con_list[glue_con_jdbc_name]
        if glue_con_jdbc_name not in glue_con_list:
            etl_functions.create_glue_con_jdbc(name= glue_con_jdbc_name,url= con[0],pwd= con[2],uid= con[1])
            print('Glue connection created')

        # create glue db
        glue_db_name = f'{svr.lower()}_{db.lower()}'
        if glue_db_name not in glue_db_list:
            etl_functions.create_glue_db(glue_db_name)
            print('Glue dbs created')

        # create workflows
        workflow_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_wf'
        if workflow_name not in glue_workflow_list:
            etl_functions.create_glue_wf(workflow_name)
            print('Workflows created')

        # create jdbc crawlers
        jdbc_crawler_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_crawler_jdbc'
        if jdbc_crawler_name not in glue_crawler_list:
            # must match original text
            jdbc_target_path = f'{db}/{sch}/{tbl}/'
            etl_functions.create_glue_crawler_jdbc(jdbc_crawler_name, glue_db_name, glue_con_jdbc_name, jdbc_target_path)
            print('JDBC crawlers created')

        # create s3 crawlers
        s3_crawler_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_crawler_s3'
        if s3_crawler_name not in glue_crawler_list:
            # must match original text
            s3_target_path = f's3://{s3_bucket_name}/{svr.lower()}/{db.lower()}/{sch.lower()}/{tbl.lower()}/'
            etl_functions.create_glue_crawler_s3(s3_crawler_name, glue_db_name, s3_target_path)
            print('S3 crawlers created')

        # create jobs
        job_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_job'
        if job_name not in glue_job_list:
            script_path = f's3://{s3_bucket_name}/scripts/glue_script.py'
            etl_functions.create_glue_job(job_name, script_path, glue_db_name, s3_bucket_name, svr.lower(), db.lower(), sch.lower(), tbl.lower())
            print('Jobs created')

        # create triggers
        trigger_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_trigger'
        trigs_to_create = etl_functions.trigs_to_create(trigger_name, workflow_name, job_name, jdbc_crawler_name, s3_crawler_name, group)
        for trig in trigs_to_create:
            if trig['name'] not in glue_trigger_list:
                etl_functions.create_glue_trigger(trig['name'], trig['workflow'], trig['type'], trig['actions'], trig['kargs'])
                print('Triggers created')

        # Start workflows
        print('Starting workflows')
        etl_functions.start_wf(workflow_name)

