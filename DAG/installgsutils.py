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

dag = DAG('install_gsutils', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

task0=BashOperator(
                    task_id='install gsutils',
                    bash_command="pip install gsutil",
                    dag=dag
                    )
task0