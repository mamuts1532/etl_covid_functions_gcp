import os
from urllib.request import urlopen
import json

from google.cloud import storage

url = os.environ['URL']
bucket_name = os.environ['BUCKET'] #without gs://
file_name = os.environ['FILE_NAME']

cf_path = '/tmp/{}'.format(file_name)

def import_file(request):
	"""Download json file from API to bucket, triggered by HTTP request.

	:param request: object contains all the data that is sent from the client to the server
	:type request: object
	:param url: url as API entry point with covid information
	:type url: environment variable
	:param bucket_name: bucket name
	:type bucket_name: environment variable
	:param file_name: downloaded file name 
	:type file_name: environment variable
	:param cf_path: path where the file is downloaded in serverless 
	:type cf_path: string

	:retunr: message
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

	with open(cf_path, 'w') as fp:
		json.dump(data_json, fp)


	# set Blob
	blob = storage.Blob(file_name, bucket)

	# upload the file to GCS
	blob.upload_from_filename(cf_path)

	request_json = request.get_json()
	if request.args and 'message' in request.args:
		return request.args.get('message')
	elif request_json and 'message' in request_json:
		return request_json['message']
	else:
		return f'Hello World!'
