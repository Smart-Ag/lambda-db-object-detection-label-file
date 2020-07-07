import update.dataloop_helpers as dataloop_helpers
from moto import mock_s3
from unittest.mock import MagicMock, patch, call
import pytest
from collections import OrderedDict


@mock_s3
def test_xml_to_dataloop_item_error_when_not_found():
    TEMP_IMG_PATH = \
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml'

    with patch("update.dataloop_helpers.dtlpy") as mock_dtlpy:
        mock_project = MagicMock()
        mock_dataset = MagicMock()
        mock_dtlpy.projects.get.return_value = mock_project
        mock_project.datasets.get.return_value = mock_dataset

        mock_dataset.items.list.return_value = [[]]
        with pytest.raises(Exception):
            dataloop_helpers.xml_to_dataloop_item(TEMP_IMG_PATH, 'Raven trial', 'all')


@mock_s3
def test_xml_to_dataloop_item():
    TEMP_IMG_PATH = \
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml'

    with patch("update.dataloop_helpers.dtlpy") as mock_dtlpy:
        mock_project = MagicMock()
        mock_dataset = MagicMock()
        mock_dtlpy.projects.get.return_value = mock_project
        mock_project.datasets.get.return_value = mock_dataset

        anno_meta_dict = {'raven-meta': ''}
        mock_item = MagicMock()
        mock_item.metadata = MagicMock()
        mock_item.metadata.__getitem__.side_effect = anno_meta_dict.__getitem__
        mock_item.metadata.__setitem__.side_effect = anno_meta_dict.__setitem__

        mock_dataset.items.list.return_value = [[mock_item]]

        dataloop_helpers.xml_to_dataloop_item(TEMP_IMG_PATH, 'Raven trial', 'all')
        print("mock_dtlpy.Box.call_args_list:", mock_dtlpy.Box.call_args_list)
        assert mock_dtlpy.Box.call_count == 3

        calls = [
            call(bottom=132, label='Combine', left=1009, right=1105, top=94),
            call(bottom=632, label='QRCode', left=577, right=611, top=605),
            call(bottom=118, label='Standing crop', left=933, right=1043, top=97)
        ]
        for it in mock_dtlpy.Box.call_args_list:
            assert it in calls

        assert mock_item.update.call_count == 1
        assert mock_item.annotations.upload.call_count == 1

        expected_meta_data = OrderedDict([
            ('annotation', OrderedDict(
                [
                    ('filename', '0242ac120002-detection-1564068022219-1.jpeg'),
                    ('size', OrderedDict([
                        ('width', '1280'), ('height', '800'), ('depth', '3')
                    ])),
                    ('segmented', '0'), ('unique', '1')
                ]))
        ])
        print("item.metadata['raven-meta']", anno_meta_dict['raven-meta'])
        print("expected_meta_data", expected_meta_data)

        assert expected_meta_data == anno_meta_dict['raven-meta']
