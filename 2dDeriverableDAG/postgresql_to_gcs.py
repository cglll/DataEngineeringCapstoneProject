import os
from airflow import DAG 
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import timedelta
from datetime import datetime

#Extract data from Postgres>Load into GCS

#default
default_args={
    'owner':'cglll',
    'depends_on past':False,
    'start_date':datetime(2021,12,1),
    'email':['cglllcglll@gmail.com'],
    'email_on_failuere':True,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=1)
}

#DAG config
dag=DAG('postgres_to_gcs',
        default_args=default_args,
        schedule_interval='@once',
        catchup=False)

#Change the identifiers
GOOGLE_CONN_ID="google_cloud_default"
POSTGRES_CONN_ID="postgres_default"
FILENAME="user.purchase"
SQL_QUERY="select * from user_purchase"
bucket_name="databootcampcglllbucket_f1ec"

upload_data=PostgresToGCSOperator(
            task_id="get_data",
            sql=SQL_QUERY,
            bucket=bucket_name,
            filename=FILENAME,
            gzip=False,
            dag=dag
)

upload_data_server_side_cursor=PostgresToGCSOperator(
            task_id="get_data_with_server_side_cursor",
            sql=SQL_QUERY,
            bucket=bucket_name,
            filename=FILENAME,
            gzip=False,
            use_server_side_cursor=True,
            export_format='parquet',
            dat=dag)

upload_data>>upload_data_server_side_cursor