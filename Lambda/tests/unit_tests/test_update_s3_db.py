import update.helpers as helpers
import update.update_s3_db as update_s3_db
from unittest.mock import MagicMock
from unittest.mock import patch
from moto import mock_s3
import boto3

FAKE_EVENT = {
    'Records': [
        {
            'eventVersion': '2.1',
            'eventSource': 'aws:s3',
            'awsRegion': 'us-east-1',
            'eventTime': '2020-01-09T20:42:04.409Z',
            'eventName': 'ObjectCreated:Put',
            'userIdentity': {
                'principalId': 'AWS:AIDAIXJEL4KSSZMBBE5FI'
            },
            'requestParameters': {
                'sourceIPAddress': '204.134.177.162'},
            'responseElements': {
                'x-amz-request-id': '908AE08CFB039AB1',
                'x-amz-id-2':
                '6fAJjAaxy4KNpmHlTiooBv4Om7Zd4tPeJWqIq7VHF3tL0m' +
                '8i2ESaXratIMrXfsSffAvhQ8nOF+sgN18/+WKxthV++zA8FTMUiLTnGtwtH/g='
            },
            's3': {
                's3SchemaVersion': '1.0',
                'configurationId': '09d9246c-9af4-48b6-bfd2-3b23f4927352',
                    'bucket': {
                        'name': 'object-detection-models-training-data-dot',
                        'ownerIdentity': {
                            'principalId': 'ABW75M62BD9NV'
                        },
                        'arn': 'arn:aws:s3:::object-detection-models-training-data-dot'
                    },
                'object': {
                        'key': '2019/7-29/0242ac120002-detection-1564068022219-1.jpeg.xml',
                        'size': 2238,
                        'eTag': '2a590649e2ec566abc12c957c3e896d5',
                        'sequencer': '005E17901D102EE642'
                        }
            }
        }
    ]
}


@mock_s3
def test_lambda_handler():

    TEMP_ANNO_PATH = \
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml'
    BUCKET_NAME = 'object-detection-models-training-data-dot'
    SRC_KEY = '2019/7-29/0242ac120002-detection-1564068022219-1.jpeg.xml'

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    helpers.upload_to_s3(TEMP_ANNO_PATH, BUCKET_NAME, SRC_KEY)

    ANNO_FILE_KEY = f'database/object_detection_label_file_item_2019/' + \
        '0242ac120002-detection-1564068022219-1.jpeg.xml.csv'
    LBL_FILE_KEY = 'database/object_detection_label_file_2019/' + \
        '0242ac120002-detection-1564068022219-1.jpeg.xml.csv'
    assert not helpers.file_exists_s3(
        BUCKET_NAME,
        ANNO_FILE_KEY)
    update_s3_db.lambda_handler(FAKE_EVENT, None)
    assert helpers.file_exists_s3(
        BUCKET_NAME,
        ANNO_FILE_KEY)
    assert helpers.file_exists_s3(
        BUCKET_NAME,
        LBL_FILE_KEY)
