# 模仿lambda 接收kinesis 不是lambda 业务

from __future__ import print_function

import json
import time

import boto3

print("测试消费kinesis 的lambda…………")


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


def read_records(kinesis_stream):
    """
    Read all the records in sent by Kinesis
    :param kinesis_stream: dict, input coming from kinesis stream
    :return:
        generator of records
    """
    records = kinesis_stream.get('Records')
    if not records:
        return []
    records_acc = (read_record(record) for record in records)
    return records_acc


def lambda_handler(event, context):
    # the stream I defined in aws console
    my_stream_name = 'debug4cli'

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
        records_json = read_records(record_response)
        # print("读到的数据--->:", records_json)

        if (record_response['Records']):
            records = record_response['Records']
            print("读到的数据--->:", records)
        # for record in records:
        #     records_acc = read_record(record)
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

        # wait for 1 seconds before looping back around to see if there is any more data to read
        time.sleep(1)


if __name__ == "__main__":
    lambda_handler("", "")
