import update.helpers as helpers
import update.update_dataloop_anno_index as update_dataloop_anno_index
from moto import mock_s3
import boto3
from unittest import mock

FAKE_EVENT = {
    'Records': [
        {
            'EventSource': 'aws:sns',
            'EventVersion': '1.0',
            'EventSubscriptionArn': 'arn:aws:sns:us-east-1:407636574581:s3_object_created:a0b78fb9-90a3-4cb3-911a-292c5112073a',  # noqa: E501
            'Sns': {
                'Type': 'Notification',
                'MessageId': 'fa710321-a87a-5f86-9578-545f1e9c772a',
                'TopicArn': 'arn:aws:sns:us-east-1:407636574581:s3_object_created',
                'Subject': None,
                'Message': '{"version":"1.0","timestamp":"2020-07-07T17:45:10.899Z","requestContext":{"requestId":"c1e49dd8-b09e-4bc9-9833-e61685bc397d","functionArn":"arn:aws:lambda:us-east-1:407636574581:function:s3_to_sns:$LATEST","condition":"Success","approximateInvokeCount":1},"requestPayload":{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2020-07-07T17:45:08.752Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAV52IIQF22IV7TYADQ"},"requestParameters":{"sourceIPAddress":"204.134.177.162"},"responseElements":{"x-amz-request-id":"D80927DDAE841B96","x-amz-id-2":"8Qpv2ENeXzlPpgZuLI1M8jzb785k7g9tltJCzvbBzKmcI/j0WyHbvwPstNxO0bSWNPrxdclf6rwaRChNDSbgcmx7Ahl3Au8/"},"s3":{"s3SchemaVersion":"1.0","configurationId":"7ee1c2d2-b50e-4c17-9946-0b07e2494b7f","bucket":{"name":"object-detection-models-training-data-dot","ownerIdentity":{"principalId":"ABW75M62BD9NV"},"arn":"arn:aws:s3:::aziz-test-deleteme"},"object":{"key":"2019/7-29/0242ac120002-detection-1564068022219-1.jpeg.xml","size":245906,"eTag":"f5561367416ef19683da525650cb285a","sequencer":"005F04B4A4ACB3D089"}}}]},"responseContext":{"statusCode":200,"executedVersion":"$LATEST"},"responsePayload":{"statusCode": 200, "body": "\\"Hello from Lambda!\\""}}',  # noqa: E501
                'Timestamp': '2020-07-07T17:45:10.962Z',
                'SignatureVersion': '1',
                'Signature': 'LTiPixt6xSP3/wdAua8+GbJLRwIvb9DoQhYd+VOJADkUuGe2T8vqy/I0Dhy0HQuEwMA00m+mgEjwC3+Lqa6OF6ZhOmuYhEqAAwbST7zRHPESsrk0UdoKITjVaS6Nu/tEedTsa+9RrkGxQd/sHkpgHdloaGMqIx43Q18PA0pO3aJGw9by6rBTOi5v0YOq7RcXEXuiMHyCOInQGh5S8pRQjE636MCiKLtuV0lQ2p7r9tSi/lWbTdqWFKAU61TUsayg5bTA5IZVZldhRDfgFaWC+1PgqHk/VBpoFKEc+5Uubt+Ph49yjJdrhzMPs6Oxz5W7y3l9d2UGa53AqN0u8QeMeg==',  # noqa: E501
                'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem',  # noqa: E501
                'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:407636574581:s3_object_created:a0b78fb9-90a3-4cb3-911a-292c5112073a',  # noqa: E501
                'MessageAttributes': {}}}]}


@mock_s3
def test_lambda_handler():

    TEMP_IMG_PATH = \
        '/io/Lambda/tests/unit_tests/test_data/0242ac120002-detection-1564068022219-1.jpeg.xml'
    BUCKET_NAME = 'object-detection-models-training-data-dot'
    SRC_KEY = '2019/7-29/0242ac120002-detection-1564068022219-1.jpeg.xml'

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    helpers.upload_to_s3(TEMP_IMG_PATH, BUCKET_NAME, SRC_KEY)

    with mock.patch("update.update_dataloop_anno_index.dataloop_helpers") as \
            mock_xml_to_dataloop_helper:

        update_dataloop_anno_index.lambda_handler(FAKE_EVENT, None)
        mock_xml_to_dataloop_helper.xml_to_dataloop_item.assert_called_once_with(
            '/tmp/0242ac120002-detection-1564068022219-1.jpeg.xml',
            "Raven trial",
            "all"
        )
