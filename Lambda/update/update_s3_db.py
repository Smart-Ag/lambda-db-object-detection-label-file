import boto3
import urllib
import update.helpers as helpers

DATA_VERSION = "2020.3.0"


def lambda_handler(event, context):
    try:
        boto3.setup_default_session(region_name='us-east-1')
        s3 = boto3.resource('s3')

        # print("event: ", event)
        # Get the object from the event and show its content type
        src_bucket = event['Records'][0]['s3']['bucket']['name']
        src_path_key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        og_filename = str(src_path_key.split('/')[-1])
        op_year = str(src_path_key.split('/')[0])
        response = helpers.create_db_entry(op_year, src_bucket, src_path_key, og_filename, s3)

        return response['ContentType']
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
