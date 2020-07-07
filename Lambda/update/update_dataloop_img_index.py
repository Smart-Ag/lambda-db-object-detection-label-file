import boto3
import urllib
import os
import json
from update.dataloop_helpers import login, get_project_dataset


def process_lambda_event(event, context):
    try:
        boto3.setup_default_session(region_name='us-east-1')
        s3_client = boto3.client('s3')
        record = event['Records']
        print("")
        print("record=", record)
        print("")
        src_bucket = event['Records'][0]['s3']['bucket']['name']
        src_path_key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        TEMP_IMG_PATH = '/tmp/'

        # Get project and dataset
        login()
        project, dataset = get_project_dataset(project_name='Raven trial', dataset_name='all')

        og_filename = str(src_path_key.split('/')[-1])

        new_file_path_temp = os.path.join(TEMP_IMG_PATH, og_filename)
        s3_client.download_file(src_bucket, src_path_key, new_file_path_temp)

        # Upload or get annotations from platform (if already exists)
        dataset.items.upload(
            local_path=new_file_path_temp,
            remote_path=src_path_key,
            overwrite=True)
        return
    except Exception as e:
        print(e)
        print(
            '[Error]: getting object {} from bucket {}. \
                Make sure they exist and your bucket is in the same region as this function.'
            .format(
                src_path_key,
                src_bucket))
        print("[Error]: event:", event)
        raise e


def lambda_handler(event, context):
    try:
        records = event['Records']

        for i, record in enumerate(records):
            if i > 0:
                print("record i=", i, "record=", record)
            if i > 0:
                print("")
            lambda_event = json.loads(record['Sns']['Message'])
            lambda_event = lambda_event['requestPayload']
            if i > 0:
                print("lambda_event: ", lambda_event)
            if i > 0:
                print("")

            process_lambda_event(lambda_event, context)

    except Exception as e:
        print(e)
        print("[Error]: event:", event)
        raise e
