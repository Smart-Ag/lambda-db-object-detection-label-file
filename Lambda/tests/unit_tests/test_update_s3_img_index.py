import update.helpers as helpers
import update.update_s3_db_img_index as update_s3_db_img_index
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
                        'name': 'object-detection-data-collection',
                        'ownerIdentity': {
                            'principalId': 'ABW75M62BD9NV'
                        },
                        'arn': 'arn:aws:s3:::object-detection-data-collection'
                    },
                'object': {
                        'key': '2019/7-29/0242ac120002-detection-1564068022219-1.jpeg',
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
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg'
    BUCKET_NAME = 'object-detection-data-collection'
    SRC_KEY = '2019/7-29/0242ac120002-detection-1564068022219-1.jpeg'

    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    helpers.upload_to_s3(TEMP_ANNO_PATH, BUCKET_NAME, SRC_KEY)

    IMG_INDEX_FILE_KEY = f'database/object_detection_img_file_2019/' + \
        '0242ac120002-detection-1564068022219-1.jpeg.csv'

    assert not helpers.file_exists_s3(
        BUCKET_NAME,
        IMG_INDEX_FILE_KEY)

    update_s3_db_img_index.lambda_handler(FAKE_EVENT, None)

    assert helpers.file_exists_s3(
        BUCKET_NAME,
        IMG_INDEX_FILE_KEY)

    img_index_response = s3.Object(BUCKET_NAME, IMG_INDEX_FILE_KEY).get()
    img_index_data = img_index_response['Body'].read().decode('ascii')
    # print(img_index_data)

    with open('/io/Lambda/tests/unit_tests/test_data/EXP_IMG_INDEX_1.csv', 'r') as f:
        expected_img_index = f.read().strip('\r').strip('\n')

    for a, b in zip(img_index_data.split("\n"), expected_img_index.split("\n")):
        # print("a: ", a)
        # print("b: ", b)
        assert a.strip('\r') == b.strip('\r')
