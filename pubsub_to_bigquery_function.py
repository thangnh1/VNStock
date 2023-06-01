from google.cloud import bigquery
import base64

DATASET_NAME = 'vnstock'
TABLE_NAME = 'subscribe_data'


def load_subscribe_data(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    encoded_data = event['data']
    data = base64.b64decode(encoded_data).decode('utf-8').split(',')

    if len(data) != 11:
        print('Error data : ', data)
        return

    client = bigquery.Client()
    table_ref = client.dataset(DATASET_NAME).table(TABLE_NAME)
    
    try:
        table =  client.get_table(table_ref)
        print('Exists', table_ref.path)
    except:
        print('Create new table...')
        schema = [
            bigquery.SchemaField('Ticker', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('Index', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('TradingDate', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('IndexChange', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('PercentIndexChange', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('ReferenceIndex', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('OpenIndex', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('CloseIndex', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('HighestIndex', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('LowestIndex', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('TotalMatchVolume', 'FLOAT', mode='NULLABLE'),
            ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)


    row = [{
        "Ticker": data[0].strip().replace("'", "").strip('['),
        "Index": float(data[1].strip().replace("'", "")),
        "TradingDate": data[2].strip().replace("'", ""),
        "IndexChange": float(data[3].strip().replace("'", "")),
        "PercentIndexChange": float(data[4].strip().replace("'", "")),
        "ReferenceIndex": float(data[5].strip().replace("'", "")),
        "OpenIndex": float(data[6].strip().replace("'", "")),
        "CloseIndex": float(data[7].strip().replace("'", "")),
        "HighestIndex": float(data[8].strip().replace("'", "")),
        "LowestIndex": float(data[9].strip().replace("'", "")),
        "TotalMatchVolume": float(data[10].strip().replace("'", "").rstrip(']'))
    }]

    errors = client.insert_rows(table, row)

    if errors == []:
        print('Complete')
    else:
        print('Error : ', errors)