import update.helpers as helpers
import boto3
from moto import mock_s3
from unittest.mock import patch
from unittest.mock import MagicMock
import pytest

DATA_VERSION = "2020.3.0"


def create_bucket(bucket_name):
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket_name)


def test_label_files_to_dynamo():

    assert True


@mock_s3
def test_label_files_to_s3():

    s3 = boto3.resource('s3')

    with open(
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml',
        'r'
    ) as content_file:
        xml_data = content_file.read()

    dest_bucket = "test-bucket"

    create_bucket(dest_bucket)

    dest_path_key_lbl = '2019/7/29/database/item.csv'
    dest_path_key_anno = '2019/7/29/database/anno.csv'
    src_full_file_path = 'test-bucket/2019/7/29/database/item.csv'
    op_year = 2019

    assert not helpers.file_exists_s3(dest_bucket, dest_path_key_lbl)
    assert not helpers.file_exists_s3(dest_bucket, dest_path_key_anno)

    with patch('update.helpers.uuid') as mock_uuid:
        with patch('update.helpers.time') as mock_time:

            mock_time.time = MagicMock(return_value=1578598487.738626)
            mock_uuid.uuid4 = MagicMock()
            mock_uuid.uuid4.side_effect = ['foo', 'bar', 'baz']

            meta_data = helpers.get_meta_data("0242ac120002-detection-1564068022219-1.jpeg.xml")

            helpers.label_files_to_csv(
                dest_bucket,
                dest_path_key_lbl,
                dest_path_key_anno,
                src_full_file_path,
                xml_data,
                meta_data,
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


expected_meta_data_2019_0_0 = {
    "version": "2019-0-0",
    "det_type": "n/a",
    "cam_func": "n/a",
    "cam_pos": "n/a",
    "cam_fov": "n/a",
    "product": "smartag",
    "op": "autocart"
}
expected_meta_data_2020_3_0 = {
    "version": "2020-3-0",
    "det_type": "nodetection",
    "cam_func": "hazard",
    "cam_pos": "left",
    "cam_fov": "112",
    "product": "dot",
    "op": "seeder"
}


@pytest.mark.parametrize("expected_meta_data,file_name", [
    (
        expected_meta_data_2019_0_0,
        "0242ac120002-detection-1564068022219-1.jpeg"
    ),
    (
        expected_meta_data_2020_3_0,
        "v2020-3-0-nodetection-hazard-left-112-dot-seeder-1578090779813.jpeg"
    ),
])
def test_get_meta_data(expected_meta_data, file_name):
    meta = helpers.get_meta_data(file_name)

    assert meta == expected_meta_data
