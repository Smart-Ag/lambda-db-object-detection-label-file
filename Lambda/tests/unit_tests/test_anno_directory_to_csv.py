from update.anno_directory_to_csv import get_annos, annos_to_csv
import os
from unittest.mock import patch
from unittest.mock import MagicMock


def test_get_annos():
    annos = get_annos("/io/Lambda/tests/unit_tests/test_data/2019/")
    # print("Annos:", annos)
    assert len(annos) == 2
    assert annos[0] == "/io/Lambda/tests/unit_tests/test_data/2019/4-10/0242ac120002-detection-1564068022219-2.jpeg.xml"
    assert annos[1] == "/io/Lambda/tests/unit_tests/test_data/2019/4-10/0242ac120002-detection-1564068022219-1.jpeg.xml"


def test_annos_to_csv():

    with patch('update.helpers.time') as mock_time:
        mock_time.time = MagicMock(return_value=1578598487.738626)

        if os.path.isfile("/io/annos.csv"):
            os.remove("/io/annos.csv")
        
        if os.path.isfile("/io/lbls.csv"):
            os.remove("/io/lbls.csv")

        annos_to_csv("/io/Lambda/tests/unit_tests/test_data/2019/", "object-detection-models-training-data")
        
        assert os.path.isfile("/io/annos.csv")
        assert os.path.isfile("/io/lbls.csv")

        # Assert annos are correct
        with open('/io/annos.csv', 'r') as f:
            actual_anno = f.read().strip('\r').strip('\n')
        with open('/io/Lambda/tests/unit_tests/test_data/EXPECTED_ANNO_MERGED.csv', 'r') as f:
            expected_anno = f.read().strip('\r').strip('\n')
        for a, b in zip(actual_anno.split("\n"), expected_anno.split("\n")):
            # print("a: ", a)
            # print("b: ", b)
            assert a.strip('\r') == b.strip('\r')

        if os.path.isfile("/io/annos.csv"):
            os.remove("/io/annos.csv")
        
        if os.path.isfile("/io/lbls.csv"):
            os.remove("/io/lbls.csv")
    

# USE THIS FUNCTION TO UPLOAD DIR TO CLOUD
# def test_annos_to_csv_ACTUAL_CLOUD_UPLOAD():

#     if os.path.isfile("/io/annos.csv"):
#         os.remove("/io/annos.csv")
    
#     if os.path.isfile("/io/lbls.csv"):
#         os.remove("/io/lbls.csv")

#     annos_to_csv("/AZIZ_DATA/2019/", "object-detection-models-training-data")
    
#     assert os.path.isfile("/io/annos.csv")
#     assert os.path.isfile("/io/lbls.csv")
