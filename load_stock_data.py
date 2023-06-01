#Load data.csv to GCS and create table bq

from google.cloud import storage, bigquery

PATH_KEY = 'vnstock-381809-22dc568a0a39.json'
BUCKET_NAME = 'vnstock-data'
DATASET_NAME = 'vnstock'
TABLE_NAME = 'vnstock_data'

storage_client = storage.Client.from_service_account_json(PATH_KEY)
storage_client.create_bucket(BUCKET_NAME)
bucket = storage_client.get_bucket(BUCKET_NAME)
bucket.blob('data.csv').upload_from_filename('data.csv')
print('Create GSC done!')

client = bigquery.Client.from_service_account_json(PATH_KEY)
dataset_ref = client.dataset(DATASET_NAME)
dataset = bigquery.Dataset(client.dataset(DATASET_NAME))
dataset = client.create_dataset(dataset)

# Define BigQuery schema
job_config = bigquery.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField('Open', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('High', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Low', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Close', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Volume', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('TradingDate', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Ticker', 'STRING', mode='NULLABLE'),
]

job_config.skip_leading_rows = 1
job_config.source_format = bigquery.SourceFormat.CSV

uri = 'gs://vnstock-data/data.csv'

load_job = client.load_table_from_uri(
                uri,
                dataset_ref.table(TABLE_NAME),
                job_config=job_config)

load_job.result()  # wait for table load to complete.
print('Job finished.')

destination_table = client.get_table(dataset_ref.table(TABLE_NAME))
print('Loaded {} rows.'.format(destination_table.num_rows))