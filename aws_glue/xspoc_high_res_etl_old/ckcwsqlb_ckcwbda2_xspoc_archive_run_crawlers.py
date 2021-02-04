# script for programmatically creating crawlers
#########################################################################
import boto3

DatabaseName = "xspoc_high_res"
Role = "arn:aws:iam::692770061892:role/service-role/AWSGlueServiceRole-s3crawler"

svr_list = ['CKCWSQLB','CKCWBDA2']
# svr_list = ['CKCWBDA2']
db_list = ['EKXSPOC','BVXSPOC','BKXSPOC','LHXSPOC','LGBXSPOC','HUXSPOC','KFXSPOC','S3XSPOC']
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
    ]

user_tbl = []
for svr in svr_list:
    for tbl in tbl_list:
        for db in db_list:
            if svr == 'CKCWSQLB':
                user_tbl.append((svr, db, 'dbo', tbl))
            elif svr == 'CKCWBDA2':
                user_tbl.append((svr, 'BDA_CKCWSQLB_' + db, 'dbo', 'dbo_' + tbl))

glue = boto3.client('glue')
response = glue.list_crawlers()
available_crawlers = response["CrawlerNames"]

for server, database, schema, table in user_tbl:

    Name = f'{server.lower()}_{database.lower()}_{schema.lower()}_{table.lower()}'
    Description = ""
    Targets = {"JdbcTargets": [
        {
            "ConnectionName": str(server.lower()),
            "Path": database.upper() + "/" + schema.lower() + "/" + table
        }
    ]
    }

    try:
        glue.create_crawler(Name=Name, Role=Role, DatabaseName=DatabaseName, Targets=Targets)
    except:
        print(Name+' already exists')
    print(Name + ' start crawler')
    glue.start_crawler(Name=Name)
