import update.helpers as helpers
import update.update_s3_db_batch as update_s3_db_batch
from moto import mock_s3
import boto3

def ListFiles(client, bucket, prefix):
    """List files in specific S3 URL"""
    response = client.list_objects(Bucket=bucket, Prefix=prefix)
    for content in response.get('Contents', []):
        yield content.get('Key')

@mock_s3
def test_test_update_s3_db_batch():
    TEMP_ANNO_PATH = \
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml'
    BUCKET_NAME = 'object-detection-models-training-data-temp'
    SRC_KEY = '2019/7-29/0242ac120002-detection-1564068022219-1.jpeg.xml'
    
    EXPECTED_ANNO_KEY = 'database/anno/object_detection_label_file_2019/0242ac120002-detection-1564068022219-1.jpeg.xml.csv'
    EXPECTED_LBL_KEY = 'database/lbl/object_detection_label_file_item_2019/0242ac120002-detection-1564068022219-1.jpeg.xml.csv'

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    helpers.upload_to_s3(TEMP_ANNO_PATH, BUCKET_NAME, SRC_KEY)
    assert helpers.file_exists_s3(
        BUCKET_NAME,
        SRC_KEY)
    
    assert not helpers.file_exists_s3(
        BUCKET_NAME,
        EXPECTED_ANNO_KEY)
    assert not helpers.file_exists_s3(
        BUCKET_NAME,
        EXPECTED_LBL_KEY)

    num_files_missing = update_s3_db_batch.create_anno_db_entries(
        BUCKET_NAME,
        key="",
        secret='')
    assert num_files_missing == 1

    file_list = ListFiles(s3_client, bucket=BUCKET_NAME, prefix="database")
    for file in file_list:
        print('File found:', file)

    assert helpers.file_exists_s3(
        BUCKET_NAME,
        EXPECTED_ANNO_KEY)
    assert helpers.file_exists_s3(
        BUCKET_NAME,
        EXPECTED_LBL_KEY)

    # assert that doing it again does not change any files, since the db index exists
    num_files_missing = update_s3_db_batch.create_anno_db_entries(
        BUCKET_NAME,
        key="",
        secret='')
    assert num_files_missing == 0

# User me to run actual uploads
# @mock_s3
# def actual_uploader():
#     pass
#     # update_s3_db_batch.create_anno_db_entries(
#     #     'object-detection-models-training-data', 
#     #     '2020',
#     #     key="AKIAV52IIQF223OU4KRG",
#     #     secret='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')