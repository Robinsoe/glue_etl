import json
import array
import awswrangler as wr
import json
import psycopg2
import awswrangler as wr
import pandas as pd
import sqlalchemy as sqla

db_host = "bda-aurora-postgresql-cluster.cluster-c2it5k2mwyhf.us-west-2.rds.amazonaws.com"
db_port = 5432
db_name = "bda-aurora"
db_user = "Postgres"
db_pass = "Den0d0BaseView4"

def create_conn():
    engine = sqla.create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name))
    conn = None
    try:
        conn = engine.connect()
    except:
        print("Cannot connect.")
    return conn

def lambda_handler(event, context):

    # Create s3 path
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path = 's3://' + bucket_name + '/' + key

    # Retrieving the df directly from Amazon S3
    df = wr.s3.read_parquet(path=path, dataset=True)

    # Retrieving the df from Athena
    # df = wr.athena.read_sql_query("SELECT * FROM my_table", database="my_db")

    # establish connection to PostgreSQL/Aurora
    conn = create_conn()

    # Push to Aurora
    table_name = 'test'
    schema_name = 'test_etl'
    df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists='replace', index=False)

    conn.close()

    return event