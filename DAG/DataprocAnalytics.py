from airflow.contrib.operators import dataproc_operator
from airflow.providers.google.cloud.operators.dataproc import (ClusterGenerator,DataprocSubmitJobOperator)
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


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 4,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
}

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="debootcampcgll",
    zone="us-central1-a",
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    image_version='1.4-debian10',
    num_workers=2,
    storage_bucket="debootcamptest",
    init_actions_uris=['gs://debootcamptest/scripts/python-setup-dataproc/pip-install.sh'],
    metadata={'PIP_PACKAGES=google-cloud-storage spark-nlp==2.7.2'},
).make()

create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    # Give the cluster a unique name by appending the date scheduled.
    # See https://airflow.apache.org/code.html#default-variables
    cluster_name='ReviewNLP',
    cluster_config=CLUSTER_GENERATOR_CONFIG,
    gcp_conn_id="google_cloud_default",
    project_id='debootcampcglll',
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