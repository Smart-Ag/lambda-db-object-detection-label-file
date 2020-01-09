import boto3
import urllib
import os
import uuid
import update.helpers as helpers


def lambda_handler(event, context):

    boto3.setup_default_session(region_name='us-east-1')
    s3 = boto3.resource('s3')

    # print("event: ", event)

    # Get the object from the event and show its content type
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    src_path_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        og_filename = str(src_path_key.split('/')[-1])
        op_year = str(src_path_key.split('/')[0])
        dest_path_key_anno = 'object_detection_label_file_' + str(op_year)
        dest_path_key_lbl = 'object_detection_label_file_item_' + str(op_year)

        src_full_file_path = os.path.join(src_bucket, src_path_key)
        
        dest_bucket = src_bucket[0:]

        dest_path_key_lbl = os.path.join(
            "database", 
            dest_path_key_lbl, og_filename + '.csv')
        dest_path_key_anno = os.path.join(
            "database", 
            dest_path_key_anno, og_filename + '.csv')

        response = s3.Object(src_bucket, src_path_key).get()
        xml_data = response['Body'].read()

        product = "smartag"
        operation = "autocart"
        if "dot" in src_bucket:
            product = "dot"
            operation = "seeder"

        helpers.label_files_to_csv(
            dest_bucket,
            dest_path_key_lbl,
            dest_path_key_anno,
            src_full_file_path,
            xml_data, 
            product, 
            operation,
            op_year)

        return response['ContentType']
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. \
                Make sure they exist and your bucket is in the same region as this function.'
            .format(
                src_path_key,
                src_bucket))
        raise e
