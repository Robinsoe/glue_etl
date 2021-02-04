# script for programmatically creating crawlers
#########################################################################
import boto3

def run_crawler(path,database,table):
    glue = boto3.client('glue_eggs', region_name='us-west-2')
    Name = database + "_" + table
    Role = "arn:aws:iam::692770061892:role/service-role/AWSGlueServiceRole-s3crawler"
    DatabaseName = database
    Description = ""
    Targets = {
        'S3Targets': [
            {
                'Path': path,
                'Exclusions': [
                ]
            },
        ]
    }
    try:
        print(Name + ' create crawler')
        glue.create_crawler(Name=Name, Role=Role, DatabaseName=DatabaseName, Targets=Targets)
    except:
        print(Name +' already exists')
    try:
        print(Name + ' start crawler')
        glue.start_crawler(Name=Name)
    except:
        print(Name + ' crawler already started')

if __name__ == '__main__':

    database = 'xspoc_high_res'
    tbl_list = [
            'tblXDiagOutputs',
            'tblXDiagResultsLast',
            'tblXDiagScores',
            'tblXDiagResults',
            'tblXDiagRodResults',
            'tblXDiagFlags',
            'tblDataHistory',
            'tblDataHistoryArchive',
            'tblCardData'
            'tblCardData_Decoded'
            ]

    for tbl in tbl_list:
        tbl = tbl.lower()
        path = 's3://' + 'xspoc-high-res' + '-glue/' + tbl

        # Run Crawler
        run_crawler(
            path=path,
            database=database,
            table=tbl
        )
