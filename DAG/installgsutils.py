import airflow
import os
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta


#default
default_args={
    'owner':'cglll',
    'depends_on past':False,
    'start_date':datetime(2021,12,1),
    'email':['cglllcglll@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG('install_gsutils', description='Install gsutils',
          schedule_interval='@once',
          start_date=datetime(2017, 3, 20), catchup=False)

task0=BashOperator(
                    task_id='install_gsutils',
                    bash_command="pip install gsutil",
                    dag=dag
                    )
task1=BashOperator(
                    task_id='install_gcloud',
                    bash_command="pip install apache-airflow-providers-google",
                    dag=dag
                    )
task0>>task1
