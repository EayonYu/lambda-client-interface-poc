# coding:utf-8
# file: _2_partner_mirror_layer_lambda_consume_kinesis.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/7/1 下午9:07
"""
description:
"""
from __future__ import print_function

import os

import simplejson as json
from boto3 import client as boto3_client

print('Loading function')

lambda_client = boto3_client('lambda')

client_interface_stream_name = os.environ.get('CLI_STREAM_NAME')
client_interface_stream_name = "debug4cli"

client_interface_stream_name = "debug4cli"


def read_record(record):
    """
    Read Kinesis record as Python dictonary
    :param record:  dict, Kinesis record
    :return: dict
    """
    # decoded_record = json.loads(record.decode())
    decoded_record = record
    return decoded_record


def invoke_event_put_lambda(partition_key, event_data):
    lambda_data = {
        'stream_name': client_interface_stream_name
        , 'partition_key': partition_key
        , 'records_to_kinesis': event_data
    }
    target_data = json.dumps(lambda_data)

    invoke_response = lambda_client.invoke(FunctionName="CloudPoc-IOT-Mirror-Kinesis-Device-Property-Put",
                                           InvocationType='RequestResponse', LogType='Tail',
                                           Payload=target_data)


def lambda_handler(event, context):

    try:
        for record in event['Records']:
            # Kinesis data is base64 encoded so decode here
            # payload = base64.b64decode(record['kinesis']['data'])
            payload = record
            # payload is bytes
            print('payload type--->: ', type(payload), payload)

            payload_str = read_record(payload)
            # payload_str is dict
            print('payload type--->: ', type(payload_str), payload_str)
            payload_data_str = payload_str.get('Data')
            payload_str = json.loads(payload_data_str.decode())
            if payload_str:
                message_type = payload_str[0].get('messageType')
                partner_id = payload_str[0].get('partnerId')

                # 开启策略
                if message_type == "DeviceDeleted.command":
                    lambda_format_data = {"payload": payload_str}
                    lambda_data = json.dumps(lambda_format_data)
                    invoke_response_1 = lambda_client.invoke(
                        FunctionName="arn:aws-cn:lambda:cn-north-1:836317673605:function:CloudPoc-IOT-Mirror-Device-Deleted",
                        InvocationType='RequestResponse', LogType='Tail',
                        Payload=lambda_data)

                    # 开始put kinesis
                    record_to_put = {
                        'action': 'DeviceDeleted',
                        'partner_id': partner_id,
                        'resource_type': 'device',
                        'body': payload_str
                    }
                    # invoke lambda
                    invoke_event_put_lambda(partner_id, record_to_put)

                elif message_type == "DevicePropertyChanged.command" or message_type == "DeviceReachability.command":
                    print("DevicePropertyChanged.command--->")

                    lambda_format_data = {"payload": payload_str}
                    lambda_data = json.dumps(lambda_format_data)
                    invoke_response = lambda_client.invoke(
                        FunctionName="arn:aws-cn:lambda:cn-north-1:836317673605:function:CloudPoc-IOT-Mirror-Device-Property-Reachability-Changed:$LATEST",
                        InvocationType='RequestResponse', LogType='Tail',
                        Payload=lambda_data)
                    property_changed_lambda_payload_res = json.loads(invoke_response['Payload'].read().decode("utf-8"))
                    print("property_changed_lambda_payload_res--->", property_changed_lambda_payload_res)
                    dynamodb_record_list_to_put = {
                        'action': 'DevicePropertyChanged',
                        'partner_id': partner_id,
                        'resource_type': 'device_property',
                        'body': property_changed_lambda_payload_res
                    }
                    invoke_event_put_lambda(partner_id, dynamodb_record_list_to_put)

                elif message_type == "DeviceEvent.command":
                    invoke_event_put_lambda(partner_id, payload_str)
                elif message_type == "DeviceInfoChanged.command":
                    lambda_format_data = {"payload": payload_str}
                    lambda_data = json.dumps(lambda_format_data)
                    invoke_response = lambda_client.invoke(
                        FunctionName="arn:aws-cn:lambda:cn-north-1:836317673605:function:CloudPoc-IOT-Mirror-Device-Info-Added-Changed",
                        InvocationType='RequestResponse', LogType='Tail',
                        Payload=lambda_data)
                    invoke_event_put_lambda(partner_id, payload_str)

                elif message_type == "DeviceAdded.command":
                    print("开始--->调用新增deviceInfo 和 property的lambda")
                    lambda_format_data = {"payload": payload_str}
                    lambda_data = json.dumps(lambda_format_data)
                    invoke_response1 = lambda_client.invoke(
                        FunctionName="arn:aws-cn:lambda:cn-north-1:836317673605:function:CloudPoc-IOT-Mirror-Device-Info-Added-Changed",
                        InvocationType='RequestResponse', LogType='Tail',
                        Payload=lambda_data)
                    invoke_response2 = lambda_client.invoke(
                        FunctionName="arn:aws-cn:lambda:cn-north-1:836317673605:function:CloudPoc-IOT-Mirror-Device-Property-Reachability-Changed:$LATEST",
                        InvocationType='RequestResponse', LogType='Tail',
                        Payload=lambda_data)
                    property_changed_lambda_payload_res = json.loads(invoke_response2['Payload'].read().decode("utf-8"))
                    print("property_changed_lambda_payload_res--->", property_changed_lambda_payload_res)
                    dynamodb_record_list_to_put = {
                        'action': 'DevicePropertyChanged',
                        'partner_id': partner_id,
                        'resource_type': 'device_property',
                        'body': property_changed_lambda_payload_res
                    }
                    invoke_event_put_lambda(partner_id, dynamodb_record_list_to_put)

                elif message_type == "DeviceListChanged.command":
                    lambda_format_data = {"payload": payload_str}
                    lambda_data = json.dumps(lambda_format_data)
                    invoke_response = lambda_client.invoke(
                        FunctionName="arn:aws-cn:lambda:cn-north-1:836317673605:function:CloudPoc-IOT-Mirror-DeviceList-Changed",
                        InvocationType='RequestResponse', LogType='Tail',
                        Payload=lambda_data)
                    invoke_event_put_lambda(partner_id, payload_str)

                else:
                    print("message_type 未匹配 --->: ", message_type)
        return 'Successfully processed {} records.'.format(len(event['Records']))
    except Exception as e:
        print(e)
        raise e
