import update.helpers as helpers
import update.update_s3_db_batch as update_s3_db_batch
from moto import mock_s3
import boto3

# User me to run actual uploads
#@mock_s3
def test_actual_uploader():
    #pass
    update_s3_db_batch.create_anno_db_entries(
        'object-detection-models-training-data',
        key="AKIAV52IIQF223OU4KRG",
        secret='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')