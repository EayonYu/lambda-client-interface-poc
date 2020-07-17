# coding:utf-8
# file: _3_device_state_dynamodb_service.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/6/29 下午1:37
"""
description:
"""

# import json
import time
from time import time as now
from decimal import *

Decimal(0.5)

import boto3
import simplejson as json
from botocore.exceptions import ClientError

_dynamodb = None

PROPERTY_NAMESPACE = 'property'
PROPERTY_NAME_SEP = '.'
PROPERTY_DEVICE_NAMESPACE = ':'


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


def create_table(table_name):
    dynamodb = get_dynamodb()

    table = dynamodb.Table(table_name)
    #
    try:
        is_table_existing = table.table_status in ("CREATING", "UPDATING",
                                                   "DELETING", "ACTIVE")
    except ClientError:
        is_table_existing = False
        print("Table %s doesn't exist." % table.name)

    if is_table_existing:
        return table

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

    sleep(2)
    return table


def read_item(table, partner_id, partner_device_id):
    print('read_item()', partner_id, partner_device_id)
    try:
        response = table.get_item(Key={'partner_id': str(partner_id), 'partner_device_id': str(partner_device_id)})
    except ClientError as e:
        print('*** Exception', e)
        print(e.response['Error']['Message'])
    else:
        # 不要打印了
        # print_item(response['Item'])
        return response['Item']


# def print_item(item):
#     print('kinesis_input--->:', type(item), item)
#     print('>>', item['partner_id'], ':', item['partner_device_id'])
#     device_state_dict = {}
#     for k in sorted(item.keys()):
#         v = item[k]
#         print('>> ', k, ':', v)
#         # if k.startswith(PROPERTY_NAMESPACE):
#         if PROPERTY_DEVICE_NAMESPACE in k:
#             device_state_dict[k] = v
#
#     device_state_object = property_dict_to_property_object(device_state_dict)
#     print_device_state_object(device_state_object)
#     return device_state_object


def print_device_state_object(device_state_object):
    print(device_state_object)
    for capability_k, capability_v in device_state_object.items():
        print(capability_k, '{')
        for property_k, property_v in capability_v.items():
            # print('>>     ', property_k, property_v)
            print('    {!r}: {!r} [updated at: {!s}]'.format(property_k, property_v['value'], property_v['updated_at']))
        print('}')


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
            # 拼接名字
            fully_qualified_property_name = \
                PROPERTY_NAMESPACE + PROPERTY_NAME_SEP + \
                capability_k + PROPERTY_NAME_SEP + \
                property_k
            property_dict[fully_qualified_property_name] = format_property_update(property_v)

    property_dict = json.loads(json.dumps(property_dict), parse_float=Decimal)

    return property_dict


def property_object_to_property_dict_v2(property_object):
    """
    Convert a JSON-like object into a list of fully-qualified properties
    ready for storage as DynamoDB item attributes
    """
    property_dict = {}
    for property_k, property_v in property_object.items():
        assert PROPERTY_NAME_SEP in property_k
        # for property_k, property_v in capability_v.items():
        #     assert PROPERTY_NAME_SEP not in property_k
        # 拼接名字
        # fully_qualified_property_name = \
        #     PROPERTY_NAMESPACE + PROPERTY_NAME_SEP + \
        #     capability_k + PROPERTY_NAME_SEP + \
        #     property_k
        fully_qualified_property_name = property_k
        property_dict[fully_qualified_property_name] = format_property_update(property_v)

    property_dict = json.loads(json.dumps(property_dict), parse_float=Decimal)

    return property_dict


# def property_dict_to_property_object(property_dict):
#     """
#     Convert a list of fully-qualified properties from storage
#     into a JSON-like object
#     """
#     property_object = {}
#     for k in sorted(property_dict.keys()):
#         v = property_dict[k]
#         tmp_k = k.split(PROPERTY_NAME_SEP)
#         assert len(tmp_k) >= 3
#         assert tmp_k[0] == PROPERTY_NAMESPACE
#         capability_name = tmp_k[1]
#         property_name = tmp_k[2]
#         if capability_name not in property_object:
#             property_object[capability_name] = {}
#         property_object[capability_name][property_name] = v
#
#         # print('k', k, 'v', v, 'property_object', property_object)
#
#     return property_object


def update_device_properties(table, partner_id, partner_device_id, property_object):
    updated_at = int(now())

    print('update_device_properties', property_object)
    property_dict = property_object_to_property_dict_v2(property_object)
    print('property_dict', property_dict)

    update_expression = 'SET {}'.format(','.join(
        f'#{i}=:{i}'
        for i in range(len(property_dict.keys()))
    ))
    update_expression += ', updated_at=:updated_at'
    print('update_expression--->： ', update_expression)

    expression_attribute_values = {
        f':{i}': v
        for i, v in enumerate(property_dict.values())
    }
    expression_attribute_values[':updated_at'] = updated_at
    print('expression_attribute_values--->: ', expression_attribute_values)

    expression_attribute_names = {
        f'#{i}': k
        for i, k in enumerate(property_dict.keys())
    }
    print('expression_attribute_names--->：', expression_attribute_names)

    response = table.update_item(
        Key={'partner_id': str(partner_id), 'partner_device_id': str(partner_device_id)},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names,
        ReturnValues='UPDATED_NEW',
    )

    print('response', response)
    return response


def populate_device(table, partner_id, partner_device_id, reachability):
    timestamp = int(now())
    item = {
        'partner_id': str(partner_id),
        'partner_device_id': str(partner_device_id),
        'created_at': timestamp,
        'updated_at': timestamp,
        'reachability': reachability,
        'deleted': False
    }
    table.put_item(Item=item)


def update_device_reachability(table, partner_id, partner_device_id, reachability):
    timestamp = int(now())
    response_item = table.update_item(
        Key={
            'partner_id': str(partner_id), 'partner_device_id': str(partner_device_id)
        },
        ExpressionAttributeNames={
            "#reachability": "reachability",
            "#updated_at": "updated_at",

        },
        ExpressionAttributeValues={
            ":reachability": reachability,
            ":updated_at": timestamp,
        },
        UpdateExpression="SET #reachability = :reachability,"
                         "#updated_at = :updated_at"

    )
    return response_item


def process_record_in_dynamodb(device_state_record):
    try:
        # capabilities_from_kinesis = device_state_record.get('capabilities')
        properties_from_kinesis = device_state_record.get('properties')

        print("properties_from_kinesis--->", properties_from_kinesis)

        table_name = 'DeviceState0709'
        table = create_table(table_name)

        # timestamp = int(now())
        partner_device_id = device_state_record.get('deviceId')
        reachability = device_state_record.get('reachability')
        #  先存入reachability
        # 都不为空，deviceStateAdd的时候
        if all([reachability, properties_from_kinesis]):
            populate_device(table, str(device_state_record.get('partnerId')), str(partner_device_id), reachability)

        #   reachability 不为空，  properties_from_kinesis 为空，只在线离线的时候
        if reachability and not properties_from_kinesis:
            response_device_reachability = update_device_reachability(table, str(device_state_record.get('partnerId')),
                                                                      str(partner_device_id), reachability)

        #     只做property改变的时候
        if properties_from_kinesis is not None:
            update_device_properties(table, 'china_iot', partner_device_id, properties_from_kinesis)

        record_to_put = read_item(table, 'china_iot', partner_device_id)
        print("record_to_put type --->: ", type(record_to_put))

        dynamodb_record_to_put = {
            'action': 'DevicePropertyChanged',
            'partner_id': 'china_iot',
            'resource_type': 'device_property',
            'body': record_to_put
        }

        # 这里改用lambda
        # put_kinesis_record("debug4RM", record_to_put_str)
        print('process_record_in_dynamodb successfully end …… ……')


        return dynamodb_record_to_put

    except Exception as e:
        print(e)
        raise e


if __name__ == "__main__":
    device_state_record = {
        "deviceId": "2030835",
        "partnerId": "china_iot",
        "reachability": {
            "value": "online1111",
            "updated_at": 1593672252675
        },
        # "deviceInfo": {
        #     "deviceName": "11空调",
        #     "tslId": "tsl-id-01-temp02",
        #     "deviceType": "DEVICE-AC",
        #     "manufacturer": "TCL",
        #     "model": "AC",
        #     "mac": "38:76:CA:44:74:28",
        #     "serialNo": "8979798hiohy986289369",
        #     "firmwareVersions": {
        #         "wifiModule": "123",
        #         "mcu": "2331"
        #     }
        # },
        "properties": {
            "2030835:temperature_controller.measured_temperature": {
                "value": 24,
                "updated_at": 1593672252675
            },
            "2030835:temperature_controller.mode": {
                "value": "cold",
                "updated_at": 1312312312
            },
            "2030835:temperature_controller.status": {
                "value": "on",
                "updated_at": 1312312999
            }
        }

    }

    dynamodb = process_record_in_dynamodb(device_state_record)
    print("--->:", dynamodb)
    import uuid

    # print(uuid.uuid1())
    #
    # print(time.time())
