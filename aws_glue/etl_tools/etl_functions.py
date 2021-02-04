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
        print('Start Delay')
        time.sleep(60)
        print('Delay Over')

def run_crawler_s3(crawler_name, s3_database, s3_path):
    check_crawler_cnt()
    targets = {
        'S3Targets': [
            {
                'Path': s3_path,
                'Exclusions': [
                ]
            },
        ]
    }
    try:
        print(crawler_name + ' create crawler')
        glue.create_crawler(Name=crawler_name, Role=role, DatabaseName=s3_database, Targets=targets)
    except:
        print(crawler_name +' already exists')
    try:
        print(crawler_name + ' start crawler')
        glue.start_crawler(Name=crawler_name)
    except:
        print(crawler_name + ' crawler already started')


def run_crawler_jdbc(crawler_name, s3_database, jdbc_server, jdbc_path):
    check_crawler_cnt()
    targets = {"JdbcTargets": [
        {
            "ConnectionName": jdbc_server,
            "Path": jdbc_path
        }
    ]
    }
    try:
        print(crawler_name + ' create crawler')
        glue.create_crawler(Name=crawler_name, Role=role, DatabaseName=s3_database, Targets=targets)
    except:
        print(crawler_name +' already exists')
    try:
        print(crawler_name + ' start crawler')
        glue.start_crawler(Name=crawler_name)
    except:
        print(crawler_name + ' crawler already started')


def delete_crawler(crawler_name):
    try:
        glue.delete_crawler(Name=crawler_name)
    except:
        print(crawler_name + ' crawler does not exist')

