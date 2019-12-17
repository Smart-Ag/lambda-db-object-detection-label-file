import json
import base64
import boto3
import urllib
import os
import update.helpers as helpers

boto3.setup_default_session(region_name='us-east-1')
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        anno_tbl = dynamodb.Table('object_detection_label_file')
        anno_item_tbl = dynamodb.Table('object_detection_label_file_item')

        full_file_path = os.path.join(bucket, key)
        response = s3.Object(bucket, key).get()

        print("response: ", response)

        xml_data = response['Body'].read()
        
        helpers.label_files_to_dynamo(full_file_path, xml_data, anno_tbl, anno_item_tbl)

        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
