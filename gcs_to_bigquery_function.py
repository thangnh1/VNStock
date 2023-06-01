from google.cloud import bigquery

DATASET_NAME = 'vnstock'
TABLE_NAME = 'vnstock_data'

def load_vnstock_data(data, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET_NAME)
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

    uri = 'gs://vnstock-data/' + data['name']

    load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(),
            job_config=job_config)

    load_job.result()
    print('Job finished.')

    destination_table = client.get_table(dataset_ref.table(TABLE_NAME))
    print('Loaded {} rows.'.format(destination_table.num_rows))