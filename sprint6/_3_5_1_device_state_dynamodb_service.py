# coding:utf-8
# @datetime:2020/6/29 下午1:37
"""
description: lambda 之间调用 和另一个（_3_5_）传参格式不一样
"""

# import json
import time
from decimal import *
from time import time as now

Decimal(0.5)

import boto3
import simplejson as json
import os
from botocore.exceptions import ClientError

_dynamodb = None

PROPERTY_NAMESPACE = 'property'
PROPERTY_NAME_SEP = '.'
# PROPERTY_DEVICE_NAMESPACE = ':'
# table_name = os.environ.get('TABLE_NAME')
table_name = "DeviceState0709"


def sleep(duration=1):
    try:
        time.sleep(duration)
    except:
        pass


def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
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
    # 格式化value值， 必须是{value："23"，"updated_at":"3453543535"}这种

    if isinstance(input_value, dict):
        if len(input_value) == 2:
            if 'value' in input_value and 'updated_at' in input_value:
                print('convert_to_property_update', 'input_value', input_value)
                return input_value

    output_value = {'value': input_value, 'updated_at': int(now())}
    # print('convert_to_property_update', 'input_value', input_value, 'output_value', output_value)
    return output_value


def format_property_obj(unformatted_property_obj) -> dict:
    if isinstance(unformatted_property_obj, dict):
        if 'deviceId' in unformatted_property_obj.keys():
            unformatted_property_obj.pop('deviceId')
        if 'partnerId' in unformatted_property_obj.keys():
            unformatted_property_obj.pop('partnerId')
        if 'messageType' in unformatted_property_obj.keys():
            unformatted_property_obj.pop('messageType')
        if 'reachability' in unformatted_property_obj.keys():
            unformatted_property_obj.pop('reachability')
        if 'deviceInfo' in unformatted_property_obj.keys():
            unformatted_property_obj.pop('deviceInfo')

    return unformatted_property_obj


def property_object_to_property_dict(property_object):
    """
    把有层次的json变为平铺的属性对象

    Convert a JSON-like object into a list of fully-qualified properties
    ready for storage as DynamoDB item attributes
    """
    property_dict = {}
    for capability_k, capability_v in property_object.items():
        assert PROPERTY_NAME_SEP not in capability_k
        for property_k, property_v in capability_v.items():
            assert PROPERTY_NAME_SEP not in property_k
            # 拼接名字 e.g.  property.temperature_controller.measured_temperature
            fully_qualified_property_name = \
                PROPERTY_NAMESPACE + PROPERTY_NAME_SEP + \
                capability_k + PROPERTY_NAME_SEP + \
                property_k
            property_dict[fully_qualified_property_name] = format_property_update(property_v)

    property_dict = json.loads(json.dumps(property_dict), parse_float=Decimal)

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

        # print('k', k, 'v', v, 'property_object', property_object)

    return property_object


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


def process_record_in_dynamodb(device_state_record_list: list):
    record_to_put_list = []
    try:
        table = create_table(table_name)
        for device_state_record in device_state_record_list:
            partner_device_id = device_state_record.get('deviceId')
            partner_id = device_state_record.get('partnerId')
            reachability = device_state_record.get('reachability')
            # snippet start 获得property_obj
            # device_state_record.pop('deviceId')
            # device_state_record.pop('partnerId')
            # device_state_record.pop('messageType')
            # if reachability:
            #     device_state_record.pop('reachability')
            # elif device_info:
            #     device_state_record.pop('deviceInfo')

            properties_from_kinesis = format_property_obj(device_state_record)
            # snippet end 获得property_obj

            print("properties_from_kinesis--->", properties_from_kinesis)
            #  先存入reachability
            # 都不为空，deviceStateAdd的时候
            if all([reachability, properties_from_kinesis]):
                populate_device(table, str(partner_id), str(partner_device_id), reachability)

            #   reachability 不为空，  properties_from_kinesis 为空，只在线离线的时候
            if reachability and not properties_from_kinesis:
                response_device_reachability = update_device_reachability(table,
                                                                          str(partner_id),
                                                                          str(partner_device_id), reachability)
            #     只做property改变的时候
            if properties_from_kinesis is not None:
                update_device_properties(table, partner_id, partner_device_id, properties_from_kinesis)

            record_to_put = read_item(table, partner_id, partner_device_id)
            print("record_to_put type --->: ", type(record_to_put))
            record_to_put_list.append(record_to_put)
        print('process_record_in_dynamodb successfully end …… ……')
    except Exception as e:
        print(e)
        raise e
    return record_to_put_list


def lambda_handler(event, context):
    # 接收参数
    payload_list = event.get('payload')

    # dynamodb处理数据
    dynamodb_record_list_to_put = process_record_in_dynamodb(payload_list)

    return dynamodb_record_list_to_put


if __name__ == "__main__":
    timestamp = int(now())
    device_state_record_v2 = [
        {
            "deviceId": "2030841",
            "temperature_controller": {
                "measured_temperature": {'value': '27', 'updated_at': timestamp - 11},
                "mode": {'value': 'cold', 'updated_at': timestamp - 12},
            },
            "wind_controller": {
                'wind_speed': {'value': 'high', 'updated_at': timestamp - 11},
                'mode': {'value': 'on', 'updated_at': timestamp - 1}
            },

            "partnerId": "china_iot",
            "messageType": "DevicePropertyChanged.command",
            "reachability": {
                "value": "offline",
                "updated_at": timestamp
            },
        }
    ]

    event = {
        "payload": device_state_record_v2
    }
dynamodb = lambda_handler(event, "")
print("--->:", dynamodb)
import uuid

# print(uuid.uuid1())
#
# print(time.time())
