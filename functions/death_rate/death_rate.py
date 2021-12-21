# import urllib library
from urllib.request import urlopen
import pandas as pd
import json
import os
from datetime import datetime
from google.cloud import bigquery

file_name = os.environ['FILE_NAME']

def death_rate(data, context):
    """Take .csv file inside bucket, does a proccess with the data and pass it to BigQuery, triggered by Cloud Storage.

    :param file_name: file name to porccess.
    :type file_name: Environment variable.
    :param bucketName: bucket name get from data trigger.
    :type bucketName: string.
    :param blobName: file name get from data trigger.
    :type blobName: string.
    :param fileName: url with path of .csv file inside bucket.
    :type fileName: string
    :param client: instance with bitquery object.
    :type client: object
    :param cf_path: url with path of .csv file stored on the serverless before load on bigquery
    :type cf_path: string
    """

    client = bigquery.Client()

    dataset_ref = client.dataset(os.environ['DATASET'])
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
                        bigquery.SchemaField('country', 'STRING'),
                        bigquery.SchemaField('deaths', 'INTEGER'),
                        bigquery.SchemaField('death_rate', 'FLOAT'),
                        bigquery.SchemaField('updated', 'STRING'),
                        ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    bucketName = data['bucket']
    blobName = data['name']
    fileName = "gs://" + bucketName + "/" + blobName

    df = pd.read_csv(fileName)
    # make new column
    df['death_rate (%)'] = (df['deaths']/df['sq_km_area'])*100
    list_today_data = ['country', 'deaths', 'death_rate (%)', 'updated']
    df = df[list_today_data]

    # # export csv
    
    cf_path = f'/tmp/{file_name}'
    df.to_csv(cf_path, index=False)


    # load the data into BQ
    with open(cf_path, "rb") as file_load:
        load_job = client.load_table_from_file(
                file_load,
                dataset_ref.table(os.environ['TABLE']),
                job_config=job_config)

    load_job.result()  # wait for table load to complete.
    print('Job 2 finished.')
