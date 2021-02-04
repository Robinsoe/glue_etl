#glue_etl ckcwsqlb_xspoc_archive_run_crawlers

from aws_glue.etl_tools import etl_setup
from aws_glue.etl_tools import etl_functions
# from etl_xspoc import etl_setup
# from etl_xspoc import etl_functions

# Run jdbc xspoc crawlers
if __name__ == '__main__':

    s3_db = 'xspoc_high_res'
    task = 'archive'
    svr_list = ['CKCWSQLB']
    db_list = etl_setup.dbs()
    sch_list = etl_setup.schs()
    tbl_list = etl_setup.tbls(task=task)
    i = 0
    for svr in svr_list:
        for tbl in tbl_list:
            for sch in sch_list:
                for db in db_list:
                    # Run Crawler jdbc
                    crawler_name = f'{svr.lower()}_{db.lower()}_{sch.lower()}_{tbl.lower()}_{task}'
                    etl_functions.run_crawler_jdbc(
                        crawler_name= crawler_name,
                        s3_database= s3_db,
                        jdbc_server= svr.lower(),
                        jdbc_path= db.upper() + "/" + sch.lower() + "/" + tbl
                        )
                    # Delete Crawler
                    # etl_functions.delete_crawler(crawler_name)