from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


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
    schedule_interval='0 9 * * *', 
)

update_data_daily = BashOperator(
        task_id='update_data_daily',
        bash_command='python /home/thangnh_info/vnstock/stock_data_daily.py',
        dag=dag
    )



update_data_daily