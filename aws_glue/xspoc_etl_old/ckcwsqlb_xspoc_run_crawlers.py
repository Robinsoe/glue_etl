# script for programmatically creating crawlers
#########################################################################
import boto3
import time

DatabaseName = "xspoc"
Role = "arn:aws:iam::692770061892:role/service-role/AWSGlueServiceRole-s3crawler"
svr_list = ['CKCWSQLB']
db_list = ['EKXSPOC','BVXSPOC','BKXSPOC','LHXSPOC','LGBXSPOC','HUXSPOC','KFXSPOC','S3XSPOC']
tbl_list = [
    'tblNodeMaster',
    'tblParameters',
    'tblEvents',
    'tblWellTests',
    'tbl_CRC_CurrentOperationState',
    'tbl_CRC_FluidShotHistory',
    'tbl_CRC_MASP',
    'tbl_CRC_CurrentWellTests',
    'tblApplications',
    'tblDisableCodes',
    'tblWellDetails',
    'tblMotorKinds',
    'tblMotorSettings',
    'tblMotorSizes',
    'tblOperationStateChangesets',
    'tblOperationStateChangesetValues',
    'tblOperationStateFields',
    'tblOperationStateFieldValues',
    'tblParamStandardTypes',
    'tblPOCTypeApplications',
    'tblPOCTypes',
    'tblPUCustom',
    'tblPumpingUnitManufacturers',
    'tblPumpingUnits',
    'tblSavedParameters',
    'tblEventGroups',
    'tblESPManufacturers',
    'tblESPMotors',
    'tblESPPumps',
    'tblESPWellDetails',
    'tblESPWellMotors',
    'tblESPWellPumps',
    # 'tblXDiagOutputs',
    # 'tblXDiagResultsLast',
    # 'tblXDiagScores',
    # 'tblXDiagResults',
    # 'tblXDiagRodResults',
    # 'tblXDiagFlags',
    # 'tblDataHistory',
    # 'tblDataHistoryArchive',
    # 'tblCardData'
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

i = 0
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

    # create crawler
    try:
        print(Name + ' create crawler')
        glue.create_crawler(Name=Name, Role=Role, DatabaseName=DatabaseName, Targets=Targets)
    except:
        print(Name+' already exists')

    # start crawler
    try:
        print(Name + ' start crawler')
        glue.start_crawler(Name=Name)
    except:
        print(Name + ' crawler already started')

    # delay every 10 crawlers
    i = i + 1
    if i == 10:
        while True:
            time.sleep(60)
            if glue.get_crawler(Name=Name)['Crawler']['State'] != 'RUNNING':
                print('Delay Over')
                i = 0
                break
