import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
import logging
import csv

##CSV to postgresi n GCP Cloud SQL Instance

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

#DAG

dag=DAG('insert_data_postgres',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path=relative_path.split("/")
    new_path=os.path.join(dir,*split_path)
    return new_path

def clean(input,output):
    with open(input, "r") as source:
        reader = csv.reader(source)
        with open(output, "w") as result:
            writer = csv.writer(result)
            for r in reader:
            # Use CSV Index to remove a column from CSV
            #r[3] = r['year']
                r.replace('"', '')
                writer.writerow((r))

def csv_to_postgres():
    #Open postgres connection
    pg_hook=PostgresHook(postgress_conn_id="postgres_default")
    get_postgres_conn=PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    #clean(file_path("user_purchase.csv"),file_path("output.csv"))
    #Load table
    with open(file_path("user_purchase.csv"),"r") as f:
        next(f)
        for row in f:
            row=row.replace('"','')
            row=row.replace("'",'')
            row_arr=row.split(",")
            if(len(row_arr)<=8):
                if (row_arr[6]==''):
                    row_arr[6]=0
                if(len(row_arr[7])>21):
                    limit = 19
                    row_arr[7] = (row_arr[7])[0:limit]
                curr.execute("""
                    INSERT INTO user_purchase
                    VALUES ('{}', '{}', '{}', '{}','{}','{}','{}','{}')
                """.format(
                row_arr[0],
                row_arr[1],
                row_arr[2],
                row_arr[3],
                row_arr[4],
                row_arr[5],
                row_arr[6],
                row_arr[7][0:19])
                )
        #curr.copy_from(f, 'user_purchase', sep=",")
        logging.info("the message you want {}".format(f))
        get_postgres_conn.commit()


GOOGLE_CONN_ID="google_cloud_default"


    #Task
task0=BashOperator(
                    task_id='authenticate_servaccount',
                    bash_command="gcloud auth activate-service-account --key-file=$json",
                    dag=dag
                    )
                    

task1=BashOperator(
                    task_id='download_file',
                    bash_command="gsutil cp gs://databootcampcglllbucket_310c/k/raw-data/user_purchase.csv .",
                    dag=dag
                    )

task2=PostgresOperator(task_id='create_table',
                        sql="""
                            CREATE TABLE IF NOT EXISTS user_purchase (
                                invoice_number varchar(10),
                                stock_code varchar(20),
                                detail varchar(1000),
                                quantity int,
                                invoice_date timestamp,
                                unit_price numeric(8,3),
                                customer_id int,
                                country varchar(20));
                                 """,
                                postgres_conn_id='postgres_default',
                                autocommit=True,
                                dag=dag)

task3=PythonOperator(task_id='csv_to_database',
                    provide_context=True,
                    python_callable=csv_to_postgres,
                    dag=dag)


task0>>task1>>task2>>task3





