import boto3
import urllib
import os
import csv
import update.helpers as helpers


def lambda_handler(event, context):

    boto3.setup_default_session(region_name='us-east-1')
    # print("event: ", event)

    # Get the object from the event and show its content type
    TEMP_IMG_PATH = '/tmp/output-img.csv'
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    src_path_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        og_filename = str(src_path_key.split('/')[-1])
        op_year = str(src_path_key.split('/')[0])
        dest_path_key = 'object_detection_img_file_' + str(op_year)

        src_full_file_path = os.path.join(src_bucket, src_path_key)

        dest_bucket = src_bucket[0:]
        dest_path_key = os.path.join(
            "database",
            dest_path_key, og_filename + '.csv')

        with open(TEMP_IMG_PATH, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(["name", "full_path"])
            writer.writerows([[og_filename, src_full_file_path]])

        print("Pushing img index to: ", os.path.join(dest_bucket, dest_path_key))
        helpers.upload_to_s3(TEMP_IMG_PATH, dest_bucket, dest_path_key)

        return
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. \
                Make sure they exist and your bucket is in the same region as this function.'
            .format(
                src_path_key,
                src_bucket))
        raise e
