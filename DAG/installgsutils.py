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

task0=BashOperator(
                    task_id='install gsutils',
                    bash_command="pip install gsutil",
                    dag=default_args
                    )
task0