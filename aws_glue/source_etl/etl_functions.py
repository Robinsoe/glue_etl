# glue_etl functions

import boto3
import time
role = "arn:aws:iam::692770061892:role/service-role/AWSGlueServiceRole-s3crawler"
s3 = boto3.client('s3', region_name='us-west-2', verify=False)
location = {'LocationConstraint': 'us-west-2'}
az = 'us-west-2b'
s3resource = boto3.resource('s3', region_name='us-west-2', verify=False)
glue = boto3.client('glue', region_name='us-west-2', verify=False)
lam = boto3.client('lambda', region_name='us-west-2', verify=False)
import os
import pyodbc
import pandas as pd
import etl_setup
from datetime import datetime, timedelta

class ETLFunctions(object):
    tag = 'data_engineering'
    # tag = 'data_science'
    glue_script_name = 'glue_script_source_to_catalog'
    lambda_script_name = 'lambda_script_catalog_to_aurora'
    s3_bucket_name = 'crcdal-glue'
    db_target = None
    s3_dir_target = None

    def __init__(self,s3_bucket_name):
        # create s3 bucket
        if s3_bucket_name not in self.s3_bucket_list():
            self.create_s3_bucket(s3_bucket_name)
        # upload glue job scripts to s3 bucket
        script_path = f'./'
        self.upload_glue_scripts_to_s3_bucket(s3_bucket_name, script_path)
        # set s3 bucket
        self.s3_bucket_name = s3_bucket_name
        #Set lists
        self.glue_db_list=self.glue_db_list()
        self.glue_con_list=self.glue_con_list()
        self.glue_crawler_list=self.glue_crawler_list()
        self.glue_jobs_list=self.glue_jobs_list()
        self.glue_wf_list=self.glue_wf_list()
        self.glue_trigger_list=self.glue_trigger_list()
        self.lambda_function_list=self.lambda_function_list()

    # Function for SQL to Dataframe
    def getSQLData(self, sql_string, connString, dataDir, dataFile, updateLocal=False):
        results = None
        if updateLocal == True:
            conn = pyodbc.connect(connString)
            results = pd.read_sql_query(sql_string, conn)
            results.to_pickle(dataDir + dataFile)
        if updateLocal == False:
            results = pd.read_pickle(dataDir + dataFile)
        return results

    # check crawler cnt and set delay
    def check_crawler_cnt(self):
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
    def s3_bucket_list(self):
        s3buckets = s3.list_buckets()
        s3bucketlist = [i['Name'] for i in s3buckets['Buckets']]
        return s3bucketlist

    # list glue databases function
    def glue_db_list(self):
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

    def glue_con_list(self):
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
    def glue_crawler_list(self):
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
    def glue_jobs_list(self):
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
    def glue_wf_list(self):
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
    def glue_trigger_list(self):
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

    # list lambdas function
    def lambda_function_list(self):
        lambdalist = []
        next_token = ''
        while True:
            if next_token == '':
                lambdas = lam.list_functions(MaxItems=50)
            else:
                lambdas = lam.list_functions(MaxItems=50, Marker=next_token)
            for lamb in lambdas['Functions']:
                lambdalist.append(lamb['FunctionName'])
            next_token = lambdas.get('NextMarker')
            if next_token is None:
                break
        return lambdalist

    # create s3 bucket function
    def create_s3_bucket(self, name):
        response = s3.create_bucket(Bucket=f'{name}', CreateBucketConfiguration=location)
        print(f'{name} S3 bucket created')
        return response

    # empty s3 bucket funciton
    def empty_s3_bucket(self, name):
        buckettoempty = s3resource.Bucket(name)
        buckettoempty.objects.all().delete()
        print(f'{name} S3 bucket emptied')

    # upload glue scripts to s3
    def upload_glue_scripts_to_s3_bucket(self, name, script_path):
        for subdir, dirs, files in os.walk(script_path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    s3.put_object(Bucket=f'{name}', Key='scripts/'+full_path[len(script_path) + 0:], Body=data)
        print('Upload scripts to s3')

    # create glue db
    def create_glue_db(self, name):
        if name not in self.glue_db_list:
            response = None
            try:
                response = glue.create_database(
                    DatabaseInput={
                        'Name': name
                    }
                )
            except:
                pass
            print(f'{name} glue dbs created')
        else:
            response = None
        return response

    # create glue con
    def create_glue_con_jdbc(self, name, url, pwd, uid):
        if name not in self.glue_con_list:
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
            print(f'{name} glue connection created')
        else:
            response = None
        return response

    # create workflows function
    def create_glue_wf(self, name):
        if name not in self.glue_wf_list:
            response = glue.create_workflow(
                Name=name,
                Description=f'Move {name} to data catalog',
                # DefaultRunProperties={
                #     'key': 'value'
                # },
                Tags={
                    'Team': self.tag,
                }
            )
            print(f'{name} workflows created')
        else:
            response = None
        return response

    # create jobs function
    def create_glue_job(self, name, script_path, kargs):

        if kargs['bookmark'] == 'Y':
            bookmark = 'job-bookmark-enable'
        else:
            bookmark = 'job-bookmark-disable'

        defaultarguments = {
            '--job-bookmark-option': bookmark,
            '--enable-metrics': '',
            '--job-language': 'python',
            '--glue_db': kargs.get('glue_db'),
            '--table_name': kargs.get('table_name'),
            '--s3_path': kargs.get('s3_path'),
            '--partition_by': str(kargs.get('partition_by', '')),
            '--bookmark':kargs.get('bookmark', ''),
        }

        connections = {
            'Connections': [
                kargs.get('connection', ''),
            ]
        }

        if name not in self.glue_jobs_list:
            response = glue.create_job(
                Name=name,
                Description=f'Move {name} to data catalog',
                Role=role,
                Command={'Name': 'glueetl',
                         'ScriptLocation': script_path
                },
                DefaultArguments=defaultarguments,
                Connections=connections,
                MaxRetries=1,
                Tags = {
                       'Team': self.tag
                },
            )
            print(f'{name} Job created')
        else:
            response = glue.update_job(
                JobName = name,
                JobUpdate = {
                    'Description':f'Move {name} to data catalog',
                    'Role':role,
                    'Command':{'Name': 'glueetl',
                             'ScriptLocation': script_path
                    },
                    'DefaultArguments':defaultarguments,
                    'Connections':connections,
                    'MaxRetries':1,
                }
            )
            print(f'{name} Job updated')
        return response

    # create crawlers function jdbc
    def create_glue_crawler_jdbc(self, name, glue_database, glue_con_jdbc, jdbc_target_path, kargs):
        self.check_crawler_cnt()
        if name not in self.glue_crawler_list:

            if kargs.get('exclusions'):
                exclusions = kargs['exclusions']
            else:
                exclusions = []

            targets = {"JdbcTargets": [
                {
                    "ConnectionName": glue_con_jdbc,
                    "Path": jdbc_target_path,
                    'Exclusions': exclusions
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
                # TablePrefix='string',
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                # Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
                # CrawlerSecurityConfiguration='string',
                Tags={
                    'Team': self.tag
                }
            )
            print(f'{name} JDBC crawlers created')
        else:
            response = None
        return response

    # create crawlers function s3
    def create_glue_crawler_s3(self, name, glue_database, s3_target_path):
        self.check_crawler_cnt()
        if name not in self.glue_crawler_list:
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
                # TablePrefix='string',
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                # Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
                # CrawlerSecurityConfiguration='string',
                Tags={
                    'Team': self.tag
                }
            )
            print(f'{name} S3 crawlers created')
        else:
            response = None
        return response

    # convert group to cron
    def set_schedule(self, group):
        # convert group to cron notation
        # cron defaults are set to 1am UTC-8
        min = 0
        hr = (datetime.strptime('1', '%H') + timedelta(hours=8)).hour
        dom = '*'
        mth = '*'
        dow = '?'
        yr = '*'
        if group in ['large', 'main', 'priority', 'unstable']:
            # run every day at 1am UTC-8
            pass
        elif group == 'day':
            # run every day at 1am UTC-8
            pass
        elif group == 'week':
            # run fist of the week at 1am UTC-8
            dow = 1
        elif group == 'month':
            # run fist of the month at 1am UTC-8
            dom = 1
        else:
            # set UTC-8 to run every day
            hr = (datetime.strptime(group[:2], '%H') + timedelta(hours=8)).hour
            min = group[2:]

        schedule = f'cron({min} {hr} {dom} {mth} {dow} {yr})'

        return schedule

    # create trigger templates to create
    def trigs_to_create(self, trigger_name, workflow_name, job_name, jdbc_crawler_name, s3_crawler_name, group):

        schedule = self.set_schedule(group)

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
    def create_glue_trigger(self, name, workflow, type, actions, kargs):

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
            triggerupdate = {
                'Description': 'string',
                'Schedule': schedule,
                'Predicate': predicate,
                'Actions': actions,
            }
        else:
            predicate = {}
            triggerupdate = {
                'Description': 'string',
                'Schedule': schedule,
                'Actions': actions,
            }

        if name not in self.glue_trigger_list:
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
                    'Team': self.tag
                }
            )
            print(f'{name} trigger created')
        else:
            response = glue.update_trigger(
                Name = name,
                TriggerUpdate = triggerupdate
            )
            print(f'{name} trigger updated')
            # response = None
        return response

    # start workflows function
    def start_wf(self, workflow):
        response = None
        wf = glue.get_workflow(Name=workflow)
        last_run= wf['Workflow'].get('LastRun')
        if last_run:
            status = last_run.get('Status')
            if status in ('RUNNING','STOPPING'):
                response = None
        else:
            while True:
                try:
                    response = glue.start_workflow_run(Name=workflow)
                    break
                except:
                    print('Workflow Delay 60 sec')
                    time.sleep(60)
                    print('Delay Over')
        return response

    # create lambda function
    def create_lambda_function(self, name, script_path, s3_key):
        response = None
        if name not in self.lambda_function_list:
            response = lam.create_function(
                FunctionName=name,
                Description=f'Move {name} to Aurora',
                Role=role,
                Code = {
                    'ZipFile': script_path,
                    'S3Bucket': self.s3_bucket_name,
                    'S3Key': s3_key,
                    'S3ObjectVersion': 'string',
                    'ImageUri': 'string'
                },
                Tags={
                    'Team': self.tag
                },
            )

        return response

    # delete workflow function
    def delete_source_to_glue_cat_etl(self, source_table_name, source_glue_db_name, s3_table_name, target_glue_db_name, jdbc_crawler_name, s3_crawler_name,
                                      job_name, trigger_name, workflow_name):

        if self.db_target == None:
            try:
                glue.delete_table(Name=source_table_name, DatabaseName=source_glue_db_name)
            except:
                pass
            try:
                glue.delete_crawler(Name=jdbc_crawler_name)
            except:
                pass

        try:
            glue.delete_table(Name=s3_table_name, DatabaseName=target_glue_db_name)
        except:
            pass
        try:
            glue.delete_crawler(Name=s3_crawler_name)
        except:
            pass
        try:
            glue.delete_job(JobName=job_name)
        except:
            pass
        try:
            for x in range(1,4):
                glue.delete_trigger(Name= trigger_name+'_'+str(x))
        except:
            pass
        try:
            glue.delete_workflow(Name=workflow_name)
        except:
            pass

    # create workflow function
    def create_source_to_glue_cat_etl(self, svr, db, sch, tbl, type, group, partition_by='None', bookmark='N', start_wf=False, delete_etl=False):

        # set inputs
        db_o = db
        sch_o = sch
        tbl_o = tbl
        svr_l = svr.lower()
        db_l = db.lower()
        sch_l = sch.lower()
        tbl_l = tbl.lower()

        # set names
        source_name = f'{svr_l}_{db_l}_{sch_l}_{tbl_l}'
        if partition_by == 'None':
            target_name = f'{svr_l}_{db_l}_{sch_l}_{tbl_l}'
        elif self.db_target:
            partition_by = partition_by.lower()
            target_name = f'{svr_l}_{db_l}_{sch_l}_{tbl_l}_{partition_by}_to_{self.db_target}'
        else:
            partition_by = partition_by.lower()
            target_name = f'{svr_l}_{db_l}_{sch_l}_{tbl_l}_{partition_by}'

        if self.db_target:
            target_glue_db_name = f'{self.db_target.lower()}'
            source_glue_db_name = f'{svr_l}_{db_l}'
        else:
            target_glue_db_name = f'{svr_l}_{db_l}'
            source_glue_db_name = target_glue_db_name
        glue_con_jdbc_name = f'{svr_l}'
        workflow_name = f'{target_name}_wf'
        jdbc_crawler_name = f'{source_name}_crawler_jdbc'
        s3_crawler_name = f'{target_name}_crawler_s3'
        job_name = f'{target_name}_job'
        db_adj = db_l.replace("-", "_")
        source_table_name= f'{db_adj}_{sch_l}_{tbl_l}'
        s3_table_name = f'{tbl_l}'
        trigger_name = f'{target_name}_trigger'
        lambda_name = f'{target_name}_lambda'

        # set paths
        con_list = etl_setup.get_con_list()
        con = con_list[glue_con_jdbc_name]
        if con[3] == 'oracle':
            jdbc_target_path = f'{db_o}/{tbl_o}/'
        else:
            jdbc_target_path = f'{db_o}/{sch_o}/{tbl_o}/'
        if self.s3_dir_target:
            s3_target_path = f's3://{self.s3_bucket_name}/{self.s3_dir_target}/{svr_l}/{db_l}/{sch_l}/{tbl_l}/'
            s3_key = f'{self.s3_dir_target}/{svr_l}/{db_l}/{sch_l}/{tbl_l}/'
        else:
            s3_target_path = f's3://{self.s3_bucket_name}/{svr_l}/{db_l}/{sch_l}/{tbl_l}/'
            s3_key = f'{svr_l}/{db_l}/{sch_l}/{tbl_l}/'
        glue_script_path = f's3://{self.s3_bucket_name}/scripts/{self.glue_script_name}.py'
        lambda_script_path = f's3://{self.s3_bucket_name}/scripts/{self.lambda_script_name}.py'

        # check for delete flag
        if delete_etl:
            self.delete_source_to_glue_cat_etl(source_table_name, source_glue_db_name,
                                               s3_table_name, target_glue_db_name,
                                               jdbc_crawler_name, s3_crawler_name,
                                                job_name, trigger_name, workflow_name)
        else:
            # create glue connections jdbc
            self.create_glue_con_jdbc(name=glue_con_jdbc_name, url=con[0], pwd=con[2], uid=con[1])

            # create glue db
            self.create_glue_db(target_glue_db_name)
            self.create_glue_db(source_glue_db_name)

            # create workflow
            self.create_glue_wf(workflow_name)

            # create jdbc crawler
            kargs = {}
            # if con[3] == 'oracle':
            #     kargs = {'exclusions':[f'']}
            self.create_glue_crawler_jdbc(jdbc_crawler_name, source_glue_db_name, glue_con_jdbc_name, jdbc_target_path, kargs)

            # create s3 crawler
            self.create_glue_crawler_s3(s3_crawler_name, target_glue_db_name, s3_target_path)

            # create job
            kargs = {'glue_db': source_glue_db_name,
                     'table_name': source_table_name,
                     's3_path': s3_target_path,
                     'partition_by': partition_by,
                     'bookmark': bookmark,
                     'connection': glue_con_jdbc_name,
                    }
            self.create_glue_job(job_name, glue_script_path, kargs)

            # create triggers
            t_to_create = self.trigs_to_create(trigger_name, workflow_name, job_name, jdbc_crawler_name, s3_crawler_name, group)
            for trig in t_to_create:
                self.create_glue_trigger(trig['name'], trig['workflow'], trig['type'], trig['actions'], trig['kargs'])

            # start workflows
            if start_wf:
                print('Starting workflows')
                self.start_wf(workflow_name)

            # create lambda function
            # self.create_lambda_function(lambda_name, lambda_script_path, s3_key)