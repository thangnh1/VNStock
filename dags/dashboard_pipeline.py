from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from looker_sdk import LookerClient, models
from google.cloud import bigquery

default_args = {
    'start_date': days_ago(1),
}

sdk = LookerClient(
    api_version="3.1",
    base_url="https://your-looker-instance.com",  # URL của Looker instance
    client_id="your-client-id",  # Client ID của ứng dụng API Looker
    client_secret="your-client-secret",  # Client Secret của ứng dụng API Looker
    verify_ssl=True,  # Xác thực SSL (True hoặc False)
)

bq_client = bigquery.Client(
    project="your-project-id",  # ID dự án BigQuery
    credentials="path/to/your/credentials.json"  # Đường dẫn đến tệp credentials của dự án BigQuery
)

def query_and_save_to_looker():
    # Truy vấn dữ liệu từ BigQuery
    query = """
    SELECT column1, column2
    FROM your_table
    """
    query_job = bq_client.query(query)
    results = query_job.result()

    # Lưu dữ liệu vào Looker
    result = sdk.create_query(
        body=models.WriteQuery(
            model="your-model-name",  # Tên model trong Looker
            view="your-view-name",  # Tên view trong Looker
            fields=["column1", "column2"],  # Các trường dữ liệu
            result_format="json",  # Định dạng kết quả
            # ... Các thông tin khác cho truy vấn
        )
    )
    query_id = result.id  # ID của truy vấn đã tạo

    # Chờ truy vấn hoàn thành
    sdk.wait_for_query(query_id)

with DAG('extract_stock_data', default_args=default_args, schedule_interval='@hourly') as dag:
    extract_task = BigQueryOperator(
        task_id='extract_data',
        sql="""
        SELECT vnstock_data.Ticker, Close, TradingDate
        FROM `vnstock-381809.vnstock.vnstock_data` AS vnstock_data
        JOIN `vnstock-381809.vnstock.stable_vnstock` AS stable_vnstock 
        ON vnstock_data.Ticker = stable_vnstock.ticker
        WHERE PARSE_DATE('%Y-%m-%d', vnstock_data.TradingDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) 
        ORDER BY vnstock_data.TradingDate DESC
        """,
        destination_dataset_table='your_destination_table',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
    )