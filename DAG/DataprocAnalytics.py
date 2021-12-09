from airflow.contrib.operators import dataproc_operator
from airflow import DAG
from datetime import datetime
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

dag = DAG('Review_analytics', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    # Give the cluster a unique name by appending the date scheduled.
    # See https://airflow.apache.org/code.html#default-variables
    cluster_name='ReviewNLP',
    num_workers=4,
    zone='us-central1',
    master_machine_type='n1-standard-4',
    worker_machine_type='n1-standard-4',
    worker_boot_disk_size=500,
    master_boot_disk_size=500,
    metadata='PIP_PACKAGES=google-cloud-storage spark-nlp==2.7.2',
    init_actions_uris=['gs://debootcamptest/scripts/python-setup-dataproc/pip-install.sh'],
    image_version='1.4-debian10',
    gcp_conn_id="google_cloud_default",
    dag=dag)

PYSPARK_JOB = {
    "reference": {"project_id": 'debootcampcglll'},
    "placement": {"cluster_name": 'ReviewNLP'},
    "pyspark_job": {"main_python_file_uri": 'gs://databootcampcglllbucket_310c/k/scripts/Jobs-dataproc/testAnalyzingmovie_reviews.py'},
}

run_dataproc_analytic_job= dataproc_operator.DataprocSubmitJobOperator(
    task_id="reviews_analytic", 
    job=PYSPARK_JOB, 
    region='us-central1', project_id='debootcampcglll'
    )

delete_dataproc_cluster=dataproc_operator.DataprocDeleteClusterOperator(
       task_id="delete_cluster", project_id='debootcampcgll', cluster_name='ReviewNLP', region='us-central1'
)
create_dataproc_cluster>>run_dataproc_analytic_job>>delete_dataproc_cluster