import urllib.request
import json
import pandas as pd
import awswrangler as wr
import boto3
import ssl

def get_secret(secret_name):
    secretsmanager = boto3.client("secretsmanager", region_name="us-west-2")
    get_secret_value_response = secretsmanager.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def retrieve_data_from_EIA(rest_api_link):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    contents = urllib.request.urlopen(rest_api_link, context=ctx).read()
    dic = json.loads(contents)
    df = pd.DataFrame(data=dic['series'][0]['data'],columns=['date','price'])
    return df

def run_crawler(path,database,table):
    glue = boto3.client('glue', region_name='us-west-2')
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


def EIA_to_s3_Data_Lake(rest_api_link, database, table):

    # Create DF
    df = retrieve_data_from_EIA(rest_api_link)

    # Send df to Aurora
    secret_name = 'bda-aurora'
    secret = get_secret(secret_name)
    engine = wr.db.get_engine(
        db_type=secret['db_type'],
        host=secret['host'],
        port=secret['port'],
        database=secret['database'],
        user=secret['username'],
        password=secret['password'])
    conn = engine.connect()
    conn.execute('TRUNCATE ' + database + '.' + table)
    wr.db.to_sql(df, engine, schema=database, name=table, if_exists='append', index=False)

    #Create s3 path
    path = 's3://'+database+'-glue/'+ table

    # Send df to Data Lake
    wr.s3.to_parquet(
        df=df,
        path=path + '/df_'+ table +'.parquet'
    )

    # Run Crawler
    run_crawler(
        path=path,
        database=database,
        table=table
    )

if __name__ == '__main__':

    database = 'eia'
    key = get_secret('eia_rest_api_key')['rest_api_key']

    table_list = [
        ('cushing_ok_crude_oil_future_contract_1_daily','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RCLC1.D'),
        ('cushing_ok_crude_oil_future_contract_2_daily','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RCLC2.D'),
        ('cushing_ok_wti_spot_price_fob_daily','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RWTC.D'),
        ('cushing_ok_wti_spot_price_fob_monthly','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RWTC.M'),
        ('cushing_ok_wti_spot_price_fob_weekly','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RWTC.W'),
        ('europe_brent_spot_price_fob_daily','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RBRTE.D'),
        ('europe_brent_spot_price_fob_monthly','https://api.eia.gov/series/?api_key='+key+'&series_id=PET.RBRTE.M'),
        ('europe_brent_spot_price_fob_weekly','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.RBRTE.W'),
        ('henry_hub_natural_gas_spot_price_daily','http://api.eia.gov/series/?api_key='+key+'&series_id=NG.RNGWHHD.D'),
        ('henry_hub_natural_gas_spot_price_monthly','https://api.eia.gov/series/?api_key='+key+'&series_id=NG.RNGWHHD.M'),
        ('henry_hub_natural_gas_spot_price_weekly','http://api.eia.gov/series/?api_key='+key+'&series_id=NG.RNGWHHD.W'),
        ('us_crude_oil_first_purchase_price_month','http://api.eia.gov/series/?api_key='+key+'&series_id=PET.F000000__3.M'),
        ('us_natural_gas_liquid_composite_price_monthly','http://api.eia.gov/series/?api_key='+key+'&series_id=NG.NGM_EPG0_PLC_NUS_DMMBTU.M'),
    ]

    for table, rest_api_link in table_list:
        try:
            EIA_to_s3_Data_Lake(rest_api_link, database, table)
        except:
            print('glue_etl failed: '+ table)

    # -------------------------------------------------------------------------------------------------------------------
    print('Finish')

