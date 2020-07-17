from django.test import Client, TestCase, override_settings
from moto import mock_dynamodb2
import boto3
import mock


@mock_dynamodb2
def mock_ddb(*args):
    dynamodb = boto3.resource('dynamodb')
    return dynamodb


@mock_dynamodb2
def mock_ddb_create_table(*args):
    dynamodb = mock_ddb()
    table = dynamodb.create_table(
        TableName='test_table',
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )



class TestMappings(TestCase):

    def setUp(self):
        self.client = Client()

    @mock.patch('app.views.create_ddb_object', side_effect=mock_ddb)
    @mock_dynamodb2
    def test_reject_supplier(self, mock_ddb_obj):
        table = mock_ddb_create_table()
        table.put_item(
            Item={
                'id': '1',
                'name': 'test'
            }
        )

        response = self.client.post('/app/view_1/', {'id': '1', })
        self.assertEqual(response.status_code, 400)



