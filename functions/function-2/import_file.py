import os
from urllib.request import urlopen
import json
import pandas as pd
from datetime import datetime

from google.cloud import storage

url = os.environ['URL']
bucket_name = os.environ['BUCKET'] #without gs://
file_name = os.environ['FILE_NAME']


def import_file(event, context):
	"""Download json file from API to bucket, convert json file to dataframe, do processing 
	and save csv file inside bucket, triggered by Cloud Pub/Sub.

	:param event: Are things that happen within your cloud environment that you might want to take action on.
	:type request: object
	:param context: Value which carries metadata about the event
	:type context: object
	:param url: url as API entry point with covid information
	:type url: environment variable
	:param bucket_name: bucket name
	:type bucket_name: environment variable
	:param file_name: downloaded file name 
	:type file_name: environment variable
	:param cf_path: path where the file is downloaded in serverless 
	:type cf_path: string

	:retunr: message with information of context from cloud functions
	:type retunr: string
	"""

	# set storage client
	client = storage.Client()

	# get bucket
	bucket = client.get_bucket(bucket_name)

	# store the response of URL
	response = urlopen(url)
		
	# storing the JSON response 
	# from url in data
	data_json = json.loads(response.read())
	data_items = data_json.items()

	# convert json to dataframe
	data_list = list(data_items)
	df = pd.DataFrame(data_list)

	#filter dataframe
	df_filtrate = df.join(pd.json_normalize(df[1])).fillna(0)
	df_filtrate = df_filtrate.drop(['All.country'], axis =1)
	df_filtrate = df_filtrate.rename(columns={0:'All.country'})
	# list with columns name 
	list_name = df_filtrate.columns.to_list()

	# filter columns name with relevant columns
	new_lis_name = [str(i) for i in list_name if 'All' in str(i)]

	# Datafraeme only relevant columns
	df_filtrate_by_all = df_filtrate[new_lis_name]
	# filter dataframe
	new_list_name_columns = df_filtrate_by_all.columns.to_list()
	new_list_name_without_all = [str(i).split('.')[1] for i in new_list_name_columns]
	df_filtrate_by_all.columns = new_list_name_without_all

	# # export json and csv with date of dowload
	today = str(datetime.date(datetime.now()))
	file_name_ext = f'{file_name}-{today}.csv'
	cf_path = f'/tmp/{file_name_ext}'
	df_filtrate_by_all.to_csv(cf_path, index=False)

	# set Blob
	blob = storage.Blob(file_name_ext, bucket)

	# upload the file to GCS
	blob.upload_from_filename(cf_path)

	print("""This Function was triggered by messageId {} published at {}""".format(context.event_id, context.timestamp))