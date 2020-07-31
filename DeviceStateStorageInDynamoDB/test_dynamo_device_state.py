import boto3
import time
from time import time as now

from botocore.exceptions import ClientError

_dynamodb = None

PROPERTY_NAMESPACE = 'property'
PROPERTY_NAME_SEP = '.'


def sleep(duration=1):
    try:
        time.sleep(duration)
    except:
        pass

def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        # _dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
        _dynamodb = boto3.resource('dynamodb')
    return _dynamodb


def delete_table(table_name):
    dynamodb = get_dynamodb()
    try:
        table = dynamodb.Table(table_name)
        table.delete()
    except Exception as e:
        print(e)


def create_table(table_name):
    dynamodb = get_dynamodb()

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'partner_device_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'partner_id',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'partner_device_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'partner_id',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    return table


def read_item(table, partner_id, partner_device_id):
    print('read_item()', partner_id, partner_device_id)
    try:
        response = table.get_item(Key={'partner_id': str(partner_id), 'partner_device_id': str(partner_device_id)})
    except ClientError as e:
        print('*** Exception', e)
        print(e.response['Error']['Message'])
    else:
        print_item(response['Item'])
        return response['Item']


def read_items(table):
    print('read_items()')
    try:
        #response = table.query(KeyConditionExpression=Key('year').eq(year))
        #response = table.query()
        response = table.scan()
    except ClientError as e:
        print('*** Exception', e)
        print(e.response['Error']['Message'])
    else:
        #print('response', response)
        print(response['Items'])
        for item in response['Items']:
            print_item(item)
        return response['Items']


def format_property_update(input_value):
    # We want to build a dictionary containing {'value': xxx, 'updated_at': yyy}
    # - If the input_value already follows the format, then return it directly
    # - If the input value is missing the 'updated_at' field, format as required

    if isinstance(input_value, dict):
        if len(input_value) == 2:
            if 'value' in input_value and 'updated_at' in input_value:
                print('convert_to_property_update', 'input_value', input_value)
                return input_value

    output_value = {'value': input_value, 'updated_at': int(now())}
    print('convert_to_property_update', 'input_value', input_value, 'output_value', output_value)
    return output_value


def property_object_to_property_dict(property_object):
    """
    Convert a JSON-like object into a list of fully-qualified properties
    ready for storage as DynamoDB item attributes
    """
    property_dict = {}
    for capability_k, capability_v in property_object.items():
        assert PROPERTY_NAME_SEP not in capability_k
        for property_k, property_v in capability_v.items():
            assert PROPERTY_NAME_SEP not in property_k

            fully_qualified_property_name = \
                PROPERTY_NAMESPACE + PROPERTY_NAME_SEP + \
                capability_k + PROPERTY_NAME_SEP + \
                property_k
            property_dict[fully_qualified_property_name] = format_property_update(property_v)

    return property_dict


def property_dict_to_property_object(property_dict):
    """
    Convert a list of fully-qualified properties from storage
    into a JSON-like object
    """
    property_object = {}
    for k in sorted(property_dict.keys()):
        v = property_dict[k]
        tmp_k = k.split(PROPERTY_NAME_SEP)
        assert len(tmp_k) >= 3
        assert tmp_k[0] == PROPERTY_NAMESPACE
        capability_name = tmp_k[1]
        property_name = tmp_k[2]
        if capability_name not in property_object:
            property_object[capability_name] = {}
        property_object[capability_name][property_name] = v

        #print('k', k, 'v', v, 'property_object', property_object)

    return property_object


def print_device_state_object(device_state_object):
    print(device_state_object)
    for capability_k, capability_v in device_state_object.items():
        print(capability_k, '{')
        for property_k, property_v in capability_v.items():
            #print('>>     ', property_k, property_v)
            print('    {!r}: {!r} [updated at: {!s}]'.format(property_k, property_v['value'], property_v['updated_at']))
        print('}')


def print_item(item):
    print('item', type(item), item)
    print('>>', item['partner_id'], ':', item['partner_device_id'])
    device_state_dict = {}
    for k in sorted(item.keys()):
        v = item[k]
        print('>> ', k, ':', v)
        if k.startswith(PROPERTY_NAMESPACE):
            device_state_dict[k] = v

    device_state_object = property_dict_to_property_object(device_state_dict)
    print_device_state_object(device_state_object)
    return device_state_object


def update_device_properties(table, partner_id, partner_device_id, property_object):
    updated_at = int(now())

    print('update_device_properties', property_object)
    property_dict = property_object_to_property_dict(property_object)
    print('property_dict', property_dict)

    update_expression = 'SET {}'.format(','.join(
        f'#{i}=:{i}'
        for i in range(len(property_dict.keys()))
    ))
    update_expression += ', updated_at=:updated_at'
    print('update_expression', update_expression)

    expression_attribute_values = {
        f':{i}': v
        for i, v in enumerate(property_dict.values())
    }
    expression_attribute_values[':updated_at'] = updated_at
    print('expression_attribute_values', expression_attribute_values)

    expression_attribute_names = {
        f'#{i}': k
        for i, k in enumerate(property_dict.keys())
    }
    print('expression_attribute_names', expression_attribute_names)

    response = table.update_item(
        Key={'partner_id': str(partner_id), 'partner_device_id': str(partner_device_id)},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names,
        ReturnValues='UPDATED_NEW',
    )

    print('response', response)
    return response


def populate_device(table, partner_id, partner_device_id):
    timestamp = int(now())
    item = {
        'partner_id': str(partner_id),
        'partner_device_id': str(partner_device_id),
        'created_at': timestamp,
        'updated_at': timestamp,
        'reachability': {
            'value': 'offline',
            'updated_at': timestamp
        },
        'deleted': False
    }
    print('item', item)
    table.put_item(Item=item)



class MyTest():
    table_name = 'DeviceState0709'

    # delete_table(table_name)
    # table = create_table(table_name)
    dynamodb = get_dynamodb()
    table = dynamodb.Table(table_name)

    print("Table status:", table.table_status)

    populate_device(table, 'china_iot', 1)
    populate_device(table, 'china_iot', 2)
    populate_device(table, 'china_iot', 3)
    # read_item(table, 'china_iot', 1)

    timestamp = int(now())

    property_object = {
        'wind_controller': {
            'wind_speed': {'value': 'low', 'updated_at': timestamp - 11},
        },
        'temperature_controller': {
            'measured_temperature': {'value': 24, 'updated_at': timestamp - 12},
        }
    }
    update_device_properties(table, 'china_iot', 1, property_object)
    read_item(table, 'china_iot', 1)

    property_object = {
        'temperature_controller': {
            'target_temperature': {'value': 24, 'updated_at': timestamp - 10},
            'mode': {'value': 'warm', 'updated_at': timestamp - 8},
        }
    }
    update_device_properties(table, 'china_iot', 1, property_object)
    read_item(table, 'china_iot', 1)

    property_object = {
        'wind_controller': {
            'wind_speed': {'value': 'high', 'updated_at': timestamp - 1},
        },
        'temperature_controller': {
            'measured_temperature': {'value': 23, 'updated_at': timestamp - 6},
            'mode': 'auto'  # {'value': 'cold', 'updated_at': timestamp-8},
            # 'mode': {'value': 'cold', 'updated_at': timestamp - 8},
        }
    }
    update_device_properties(table, 'china_iot', 1, property_object)
    read_item(table, 'china_iot', 1)


if __name__ == "__main__":
    MyTest()

# EOF
