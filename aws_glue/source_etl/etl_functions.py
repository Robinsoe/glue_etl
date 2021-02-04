# glue_etl functions

import boto3
import time
role = "arn:aws:iam::692770061892:role/service-role/AWSGlueServiceRole-s3crawler"
s3 = boto3.client('s3', region_name='us-west-2', verify=False)
location = {'LocationConstraint': 'us-west-2'}
az = 'us-west-2b'
s3resource = boto3.resource('s3', region_name='us-west-2', verify=False)
glue = boto3.client('glue', region_name='us-west-2', verify=False)
import os
import pyodbc
import pandas as pd

tag = 'data_engineering'
# tag = 'data_science'

# Function for SQL to Dataframe
def getSQLData(sql_string, connString, dataDir, dataFile, updateLocal=False):
    results = None
    if updateLocal == True:
        conn = pyodbc.connect(connString)
        results = pd.read_sql_query(sql_string, conn)
        results.to_pickle(dataDir + dataFile)
    if updateLocal == False:
        results = pd.read_pickle(dataDir + dataFile)
    return results

# check crawler cnt and set delay
def check_crawler_cnt():
    next_token = ""
    cnt = 0
    while True:
        responseGetCrawlers = glue.get_crawler_metrics(MaxResults=200, NextToken=next_token)
        for c in responseGetCrawlers.get('CrawlerMetricsList'):
            if c.get('StillEstimating'):
                cnt = cnt + 1
        next_token = responseGetCrawlers.get('NextToken')
        if next_token is None:
            break

    if cnt >=10:
        print('Crawler Delay 60 sec')
        time.sleep(60)
        print('Delay Over')

# list crawlers function
def s3_bucket_list():
    s3buckets = s3.list_buckets()
    s3bucketlist = [i['Name'] for i in s3buckets['Buckets']]
    return s3bucketlist

# list glue databases function
def glue_db_list():
    dblist = []
    next_token = ''
    while True:
        dbs = glue.get_databases(MaxResults=100, NextToken=next_token)
        for db in dbs['DatabaseList']:
            dblist.append(db['Name'])
        next_token = dbs.get('NextToken')
        if next_token is None:
            break
    return dblist

def glue_con_list():
    conlist = []
    next_token = ''
    while True:
        cons = glue.get_connections(MaxResults=100, NextToken=next_token)
        for con in cons['ConnectionList']:
            conlist.append(con['Name'])
        next_token = cons.get('NextToken')
        if next_token is None:
            break
    return conlist

# list crawlers function
def glue_crawler_list():
    crawlerlist = []
    next_token = ''
    while True:
        crawlers = glue.get_crawlers(MaxResults=100, NextToken=next_token)
        for crawler in crawlers['Crawlers']:
            crawlerlist.append(crawler['Name'])
        next_token = crawlers.get('NextToken')
        if next_token is None:
            break
    return crawlerlist

# list jobs function
def glue_jobs_list():
    joblist = []
    next_token = ''
    while True:
        jobs = glue.get_jobs(MaxResults=100, NextToken=next_token)
        for job in jobs['Jobs']:
            joblist.append(job['Name'])
        next_token = jobs.get('NextToken')
        if next_token is None:
            break
    return joblist

# list workflows function
def glue_wf_list():
    wflist = []
    next_token = ''
    while True:
        wfs = glue.list_workflows(MaxResults=25, NextToken=next_token)
        for wf in wfs['Workflows']:
            wflist.append(wf)
        next_token = wfs.get('NextToken')
        if next_token is None:
            break
    return wflist

# list triggers function
def glue_trigger_list():
    triggerlist = []
    next_token = ''
    while True:
        triggers = glue.get_triggers(MaxResults=25, NextToken=next_token)
        for trigger in triggers['Triggers']:
            triggerlist.append(trigger['Name'])
        next_token = triggers.get('NextToken')
        if next_token is None:
            break
    return triggerlist

# create s3 bucket function
def create_s3_bucket(name):
    response = s3.create_bucket(Bucket=f'{name}', CreateBucketConfiguration=location)
    return response

# empty s3 bucket funciton
def empty_s3_bucket(name):
    buckettoempty = s3resource.Bucket(name)
    buckettoempty.objects.all().delete()

# upload glue scripts to s3
def upload_glue_scripts_to_s3_bucket(name, script_path):
    # filename = path
    # s3.put_object(Bucket=f'{name}', Key=f'scripts/')
    # s3.upload_file(Bucket=f'{name}', Key=f'scripts/{filename}', Filename=path)
    for subdir, dirs, files in os.walk(script_path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                s3.put_object(Bucket=f'{name}', Key='scripts/'+full_path[len(script_path) + 0:], Body=data)

# create glue db
def create_glue_db(name):
    response = None
    try:
        response = glue.create_database(
            DatabaseInput={
                'Name': name
            }
        )
    except:
        pass
    return response

# create glue con
def create_glue_con_jdbc(name, url, pwd, uid):
    response = glue.create_connection(
        ConnectionInput={
            'Name': name,
            'Description': '',
            'ConnectionType': 'JDBC',
            'ConnectionProperties': {
                'JDBC_CONNECTION_URL': url,
                'JDBC_ENFORCE_SSL': 'false',
                'PASSWORD': pwd,
                'USERNAME': uid
            },
            'PhysicalConnectionRequirements': {
                'SubnetId': 'subnet-0bea400067c5cb1d0',
                'SecurityGroupIdList': [
                    'sg-05ee4c86000c15345',
                ],
                'AvailabilityZone': az
            }
        }
    )
    return response

# create workflows function
def create_glue_wf(name):
    response = glue.create_workflow(
        Name=name,
        Description=f'Move {name} to data catalog',
        # DefaultRunProperties={
        #     'key': 'value'
        # },
        Tags={
            'Team': tag,
        }
    )
    return response

# create jobs function
def create_glue_job(name, script_path, glue_db, s3_bucket, svr, db, sch, tbl):

    bookmark = 'job-bookmark-disable'

    response = glue.create_job(
        Name=name,
        Description=f'Move {name} to data catalog',
        Role=role,
        Command={'Name': 'glueetl',
                 'ScriptLocation': script_path
        },
        DefaultArguments={
            '--job-bookmark-option': bookmark,
            '--enable-metrics': '',
            '--job-language': 'python',
            '--glue_db': glue_db,
            '--s3_bucket': s3_bucket,
            '--svr': svr,
            '--db': db,
            '--sch': sch,
            '--tbl': tbl,
        },
        Connections={
            'Connections': [
                svr,
            ]
        },
        MaxRetries=1,
        Tags = {
               'Team': tag
        },
        )
    return response

# create crawlers function jdbc
def create_glue_crawler_jdbc(name, glue_database, glue_con_jdbc, jdbc_target_path):
    check_crawler_cnt()

    targets = {"JdbcTargets": [
        {
            "ConnectionName": glue_con_jdbc,
            "Path": jdbc_target_path
        }
    ]
    }

    response = glue.create_crawler(
        Name=name,
        Role=role,
        DatabaseName=glue_database,
        Description=f'Crawls {name}',
        Targets=targets,
        # Schedule='string',
        Classifiers=[],
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        # Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        # CrawlerSecurityConfiguration='string',
        Tags={
            'Team': tag
        }
    )

    return response

# create crawlers function s3
def create_glue_crawler_s3(name, glue_database, s3_target_path):
    check_crawler_cnt()

    targets = {
        'S3Targets': [
            {
                'Path': s3_target_path,
                'Exclusions': [
                ]
            },
        ]
    }

    response = glue.create_crawler(
        Name=name,
        Role=role,
        DatabaseName=glue_database,
        Description=f'Crawls {name}',
        Targets=targets,
        # Schedule='string',
        Classifiers=[],
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        # Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        # CrawlerSecurityConfiguration='string',
        Tags={
            'Team': tag
        }
    )
    return response

# create trigger templates to create
def trigs_to_create(trigger_name, workflow_name, job_name, jdbc_crawler_name, s3_crawler_name, group):

    # convert group to cron notation
    if group in ['large','main','priority','unstable']:
        schedule = 'cron(00 01 * * ? *)'
    else:
        hr = group[:2]
        min = group[-2]
        schedule = f'cron({min} {hr} * * ? *)'

    trigs_to_create = [{
        'name': trigger_name + '_1',
        'workflow': workflow_name,
        'type': 'SCHEDULED',
        'actions': [{'CrawlerName': jdbc_crawler_name}],
        'kargs': {
            'schedule': schedule
        }
    },
        {
            'name': trigger_name + '_2',
            'workflow': workflow_name,
            'type': 'CONDITIONAL',
            'actions': [{'JobName': job_name}],
            'kargs': {
                'logical': 'ANY',
                'conditions': [{'LogicalOperator': 'EQUALS', 'CrawlerName': jdbc_crawler_name, 'CrawlState': 'SUCCEEDED'}]
            }
        },
        {
            'name': trigger_name + '_3',
            'workflow': workflow_name,
            'type': 'CONDITIONAL',
            'actions': [{'CrawlerName': s3_crawler_name}],
            'kargs': {
                'logical': 'ANY',
                'conditions': [{'LogicalOperator': 'EQUALS', 'JobName': job_name, 'State': 'SUCCEEDED'}]
            }
        }
    ]
    return trigs_to_create

# create triggers function
def create_glue_trigger(name, workflow, type, actions, kargs):
    if type == 'ON_DEMAND':
        StartOnCreationBool = False
    else:
        StartOnCreationBool = True

    if type == 'SCHEDULED':
        schedule = kargs['schedule']
    else:
        schedule = ''

    if type == 'CONDITIONAL':
        predicate = {
            'Logical': kargs['logical'],
            'Conditions': kargs['conditions']
        }
    else:
        predicate = {}

    response = glue.create_trigger(
        Name=name,
        WorkflowName=workflow,
        Type=type,
        Schedule=schedule,
        Predicate=predicate,
        Actions=actions,
        Description='string',
        StartOnCreation=StartOnCreationBool,
        Tags={
            'Team': tag
        }
    )
    return response

# start workflows function
def start_wf(workflow):
    response = None
    while True:
        try:
            response = glue.start_workflow_run(Name=workflow)
            break
        except:
            print('Workflow Delay 60 sec')
            time.sleep(60)
            print('Delay Over')
    return response

# delete workflows function
def delete_wf_all(glue_db, table, s3_table, jdbc_crawler, s3_crawler, job, trigger, workflow):
    try:
        glue.delete_table(Name=table, DatabaseName=glue_db)
    except:
        pass
    try:
        glue.delete_table(Name=s3_table, DatabaseName=glue_db)
    except:
        pass
    try:
        glue.delete_crawler(Name=jdbc_crawler)
    except:
        pass
    try:
        glue.delete_crawler(Name=s3_crawler)
    except:
        pass
    try:
        glue.delete_job(JobName=job)
    except:
        pass
    try:
        for x in range(1,4):
            glue.delete_trigger(Name= trigger+'_'+str(x))
    except:
        pass
    try:
        glue.delete_workflow(Name=workflow)
    except:
        pass

