# 模仿lambda 接收kinesis 不是lambda 业务

from __future__ import print_function

import json
import time

import boto3

from _2_partner_mirror_layer_lambda_consume_kinesis import lambda_handler as target_lambda


def read_record(record):
    """
    Read Kinesis record as Python dictonary
    :param record:  dict, Kinesis record
    :return:
        dict
    """
    decoded_record = json.loads(record.decode())
    # add
    # decoded_record['event_id'] = record['kinesis']['sequenceNumber']
    return decoded_record


# def read_records(kinesis_stream):
#     """
#     Read all the records in sent by Kinesis
#     :param kinesis_stream: dict, input coming from kinesis stream
#     :return:
#         generator of records
#     """
#     records = kinesis_stream.get('Records')
#     if not records:
#         return []
#     records_acc = (read_record(record) for record in records)
#     return records_acc


def lambda_handler(event, context):
    # the stream I defined in aws console
    my_stream_name = 'debug4partner_batch'

    kinesis_client = boto3.client('kinesis', region_name='cn-north-1')

    response = kinesis_client.describe_stream(StreamName=my_stream_name)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    # We use ShardIteratorType of LATEST which means that we start to look
    # at the end of the stream for new incoming data. Note that this means
    # you should be running the this lambda before running any write lambdas
    #
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                       ShardId=my_shard_id,
                                                       ShardIteratorType='LATEST')

    # get your shard number and set up iterator
    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=100)

    while 'NextShardIterator' in record_response:
        # read up to 100 records at a time from the shard number
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=100)
        # Print only if we have something
        if (record_response['Records']):
            records = record_response['Records']
            # for record in records:
            #     # records_acc = read_record(record)
            #     print("--->:  ", type(record))
            #     data = record['Data']
            #     # bytes
            #     print('--->', type(data), data)
            #     # json
            #     records_acc = read_record(data)
            #     print(type(records_acc), records_acc)
            #     # records_acc['action'],records_acc['resource_type'], records_acc['body']
            #     print(records_acc['action'], records_acc['resource_type'], records_acc['body'])
            #     # 开始store dynamodb
            event = {
                "Records": records
            }
            context = {}
            target_lambda(event, context)

        # wait for 1 seconds before looping back around to see if there is any more data to read
        time.sleep(1)


if __name__ == "__main__":
    event = {

    }
    context = {

    }
    lambda_handler(event, context)

    # data = b'{"action": "DevicePropertyChanged", "partner_name": "china_iot", "resource_type": "device_state", "body": "{\\"updated_at\\": 1593507528, \\"created_at\\": 1593507528, \\"deleted\\": false, \\"partner_device_id\\": \\"32423422\\", \\"partner_id\\": \\"china_iot\\", \\"property.wind_controller.wind_speed\\": {\\"updated_at\\": 999999, \\"value\\": \\"auto\\"}, \\"reachability\\": {\\"updated_at\\": 888888, \\"value\\": \\"online\\"}, \\"property.temperature_controller.measured_temperature\\": {\\"updated_at\\": 777777, \\"value\\": 31}}"}'
    # print(type(data))
    # record = read_record(data)
