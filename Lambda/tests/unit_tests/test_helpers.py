import update.helpers as helpers
import boto3
from moto import mock_s3
import csv
from unittest.mock import patch
from unittest.mock import MagicMock

def create_bucket(bucket_name):
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)

def test_label_files_to_dynamo():

    assert True


@mock_s3
def test_label_files_to_s3():

    s3 = boto3.resource('s3')
    
    full_file_path = 'full_path'

    with open('/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml', 'r') as content_file:
        xml_data = content_file.read()

    dest_bucket = "test-bucket"

    create_bucket(dest_bucket)

    dest_path_key_lbl = '2019/7/29/database/item.csv'
    dest_path_key_anno = '2019/7/29/database/anno.csv'
    src_full_file_path = 'test-bucket/2019/7/29/database/item.csv'
    product = "dot"
    operation = "seeder"
    op_year = 2019

    assert not helpers.file_exists_s3(dest_bucket, dest_path_key_lbl)
    assert not helpers.file_exists_s3(dest_bucket, dest_path_key_anno)

    with patch('update.helpers.uuid') as mock_uuid:
        with patch('update.helpers.time') as mock_time:

            mock_time.time = MagicMock(return_value=1578598487.738626)
            mock_uuid.uuid4 = MagicMock()
            mock_uuid.uuid4.side_effect = ['foo', 'bar', 'baz']

            helpers.label_files_to_csv(
                dest_bucket,
                dest_path_key_lbl,
                dest_path_key_anno,
                src_full_file_path,
                xml_data,
                product,
                operation,
                op_year)

            with open('/io/Lambda/tests/unit_tests/test_data/EXP_ANNO_1.csv', 'r') as f:
                expected_anno = f.read().strip('\r').strip('\n')

            with open('/io/Lambda/tests/unit_tests/test_data/EXP_LBL_1.csv', 'r') as f:
                expected_lbl = f.read().strip('\r').strip('\n')


            assert helpers.file_exists_s3(dest_bucket, dest_path_key_lbl)
            lbl_response = s3.Object(dest_bucket, dest_path_key_lbl).get()
            lbl_data = lbl_response['Body'].read().decode('ascii')

            for a, b in zip(lbl_data.split("\n"), expected_lbl.split("\n")):
                # print("a: ", a)
                # print("b: ", b)
                assert a.strip('\r') == b.strip('\r')

            assert helpers.file_exists_s3(dest_bucket, dest_path_key_anno)
            anno_response = s3.Object(dest_bucket, dest_path_key_anno).get()
            anno_data = anno_response['Body'].read().decode('ascii')
            for a, b in zip(anno_data.split("\n"), expected_anno.split("\n")):
                    # print("a: ", a)
                    # print("b: ", b)
                assert a.strip('\r') == b.strip('\r')
