from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['nguyenthang187txnm@gmail.com'],
    'catchup': False
}

dag = DAG(
    'update_data_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 9-11,13-14 * * *')


update_data_subscribe = BashOperator(
    task_id='update_data_subscribe',
    bash_command='python /home/thangnh_info/vnstock/publish_data_subcribe.py',
    dag=dag
)


update_data_subscribe