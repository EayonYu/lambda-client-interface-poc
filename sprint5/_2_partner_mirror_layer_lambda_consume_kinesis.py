# coding:utf-8
# file: _2_partner_mirror_layer_lambda_consume_kinesis.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/7/1 下午9:07
"""
description:
"""
from __future__ import print_function

import base64
import decimal
import json
from boto3 import client as boto3_client
from _3_device_state_dynamodb_service import process_record_in_dynamodb

print('kinesis 消费 Loading function')

lambda_client = boto3_client('lambda')


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def read_record(record):
    """
    Read Kinesis record as Python dictonary
    :param record:  dict, Kinesis record
    :return:
        dict
    """
    # decoded_record = json.loads(record.decode())
    decoded_record = record
    # add
    # decoded_record['event_id'] = record['kinesis']['sequenceNumber']
    return decoded_record


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        # payload = base64.b64decode(record['kinesis']['data'])
        payload = record
        # payload is bytes
        print('payload type--->: ', type(payload))
        print("Decoded payload: ", payload)

        payload_str = read_record(payload)
        # payload_str is dict
        print('payload type--->: ', type(payload_str))
        print("Decoded payload_str: ", payload_str)
        payload_data_str = payload_str.get('Data')
        payload_str = json.loads(payload_data_str.decode())

        # 组装数据 开始调用 store in dynamodb 的lambda
        # 就是调用第三部lambda
        record_to_put_str = process_record_in_dynamodb(payload_str)
        # 开始put kinesis
        data = {
            'stream_name': 'debug4cli'
            , 'partition_key': 'china_iot'
            , 'records_to_kinesis': record_to_put_str
        }
        target_data = json.dumps(data, cls=DecimalEncoder)

        invoke_response = lambda_client.invoke(FunctionName="CloudPoc-IOT-Mirror-Kinesis-Device-Property-Put",
                                               InvocationType='RequestResponse', LogType='Tail', Payload=target_data)
        payload_res = json.loads(invoke_response['Payload'].read().decode("utf-8"))
        print("payload_res--->", payload_res)

    print('Successfully processed {} records')
    # return 'Successfully processed {} records.'.format(len(event['Records']))
