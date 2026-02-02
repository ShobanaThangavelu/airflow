# import the libraries
from datetime import timedelta,datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy


#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner':'Shobana Thangavelu',
    'start_date': datetime(26,2,2),
    'email': ['shobana@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

# define the DAG
dag = DAG(
    'Shobana_Second_DAG',
    default_args=default_args,
    description='My second DAG',
    schedule=timedelta(days=1),
)

# define the tasks

# define the first task

extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > ~/extracted-data.txt',
    dag=dag,
)


# define the second task
transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < ~/extracted-data.txt > ~/transformed-data.csv',
    dag=dag,
)


# task pipeline
extract >> transform_and_load
