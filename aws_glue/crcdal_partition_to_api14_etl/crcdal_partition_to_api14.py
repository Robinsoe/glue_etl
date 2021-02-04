
import pandas as pd
import awswrangler as wr

if __name__ == '__main__':
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

    data = {'servername':['aurora'],
            'databasename':['bda-aurora'],
            'schemaname': ['bda'],
            'tablename':['v_well_info_general_detail_with_crc_fields'],
            'migrationtype': [''],
            'groupno': ['main'],
            'apivar': ['api_no14']
            }
    df = pd.DataFrame(data)

    # Loop through utility table
    for index, row in df.iterrows():
        svr = row['servername']
        db = row['databasename']
        sch = row['schemaname']
        tbl = row['tablename']
        type = row['migrationtype']
        group = row['groupno']
        api = row['apivar']
        print(f'Partition ETL: {db.lower()}_{sch.lower()}_{tbl.lower()}')

        s3_bucket_name = 'crcdal-glue'
        s3_path ='s3://' + s3_bucket_name + '/' + svr + '/' + db + '/' + sch + '/' + tbl
        output = wr.s3.read_parquet(path=s3_path)
        wr.s3.to_parquet(df=output,
                            path=f's3://crcdal-well-data/source-glue/{sch}/{tbl}/',
                            mode='append',
                            dataset=True,
                            partition_cols=[api],
                            database='crcdalv2',
                            table=f'glue_{sch}_{tbl}',
                            description=f'Glue data for {sch}.{tbl}',
                            parameters={'Team': 'strategic_analytics'},
                            concurrent_partitioning=False
                          )
