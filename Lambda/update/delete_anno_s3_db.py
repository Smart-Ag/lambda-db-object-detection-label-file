import boto3
import urllib
import os


def lambda_handler(event, context):

    boto3.setup_default_session(region_name='us-east-1')
    s3 = boto3.resource('s3')

    # Get the object from the event and show its content type
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    src_path_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        og_filename = str(src_path_key.split('/')[-1])
        op_year = str(src_path_key.split('/')[0])
        dest_path_key_anno = 'anno/object_detection_label_file_' + str(op_year)
        dest_path_key_lbl = 'lbl/object_detection_label_file_item_' + str(op_year)

        src_path_key_anno = os.path.join(
            "database",
            dest_path_key_anno, og_filename + '.csv')

        print("Deleting anno from: ", src_bucket, '/', src_path_key_anno)
        try:
            s3.Object(src_bucket, src_path_key_anno).delete()
        except Exception as e:
            print("Deleting the annotation db file failed: ", src_bucket, src_path_key_anno)
            print("Error: ", e)
            raise e

        src_path_key_lbl = os.path.join(
            "database",
            dest_path_key_lbl, og_filename + '.csv')

        print("Deleting lbl from: ", src_bucket, '/', src_path_key_lbl)
        try:
            s3.Object(src_bucket, src_path_key_lbl).delete()
        except Exception as e:
            print("Deleting the lbl db file failed: ", src_bucket, src_path_key_lbl)
            print("Error: ", e)
            raise e

        return 200
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. \
                Make sure they exist and your bucket is in the same region as this function.'
            .format(
                src_path_key,
                src_bucket))
        raise e
