import os
from google.cloud import bigquery

def load_csv_to_bq(data, context):
	"""Take .csv file inside bucket and pass it to BigQuery, triggered by Cloud Storage.

	:param client: instance with bitquery object.
	:type client: object
	:param dataset_ref: Environment variable setting with dataset name in bigquery
	:type dataset_ref: object
	:param uri: url with path of .csv file inside bucket. 
	:type uri: string
	"""

    client = bigquery.Client()

    dataset_ref = client.dataset(os.environ['DATASET'])
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
            bigquery.SchemaField('country', 'STRING'),
            bigquery.SchemaField('confirmed', 'INTEGER'),
            bigquery.SchemaField('recovered', 'INTEGER'),
            bigquery.SchemaField('deaths', 'INTEGER'),
            bigquery.SchemaField('population', 'FLOAT'),
            bigquery.SchemaField('sq_km_area', 'FLOAT'),
            bigquery.SchemaField('life_expectancy', 'FLOAT'),
            bigquery.SchemaField('elevation_in_meters', 'STRING'),
            bigquery.SchemaField('continent', 'STRING'),
            bigquery.SchemaField('abbreviation', 'STRING'),
            bigquery.SchemaField('location', 'STRING'),
            bigquery.SchemaField('iso', 'FLOAT'),
            bigquery.SchemaField('capital_city', 'STRING'),
            bigquery.SchemaField('lat', 'FLOAT'),
            bigquery.SchemaField('long', 'FLOAT'),
            bigquery.SchemaField('updated', 'STRING'),
            ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    # get the URI for uploaded CSV in GCS from 'data'
    uri = 'gs://' + data['bucket'] + '/' + data['name']

    # load the data into BQ
    load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(os.environ['TABLE']),
            job_config=job_config)

    load_job.result()  # wait for table load to complete.
    print('Job finished.')
