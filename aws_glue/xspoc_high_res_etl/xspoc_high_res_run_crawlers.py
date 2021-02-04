# glue_etl xspoc_high_res_run_crawlers

from aws_glue.etl_tools import etl_setup
from aws_glue.etl_tools import etl_functions
# from etl_xspoc import etl_setup
# from etl_xspoc import etl_functions

# Run s3 xspoc crawlers
if __name__ == '__main__':
    s3_db = 'xspoc_high_res'
    s3_bucket = 'xspoc-high-res-glue'
    task = 'archive'
    tbl_list = [x.lower() for x in etl_setup.tbls(task=task)]
    tbl_list.append(('tblCardData_Decoded').lower())
    for tbl in tbl_list:
        # Run Crawler s3
        etl_functions.run_crawler_s3(
            crawler_name= s3_db + "_" + tbl,
            s3_database= s3_db,
            s3_path= 's3://' + s3_bucket + '/' + tbl
            )
