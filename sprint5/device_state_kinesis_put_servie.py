import json

import boto3

# The kinesis stream already defined in asw console
kinesis_client = boto3.client('kinesis', region_name='cn-north-1')


def put_to_kinesis_stream(stream_name, partition_key, records_to_kinesis):
    try:

        print("records_to_kinesis--->: ", records_to_kinesis)

        put_response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(records_to_kinesis),
            PartitionKey=partition_key
        )
        return put_response

        # records_to_kinesis = list()
        #
        # records_to_kinesis.append({
        #     'Data': json.dumps({
        #         'action': 'DevicePropertyChanged',
        #         'partner_id': 'china_iot',
        #         'resource_type': 'device_state',
        #         'body': data
        #     }),
        #     'PartitionKey': 'china_iot'
        # })

        # report_doing(device_state_kinesis_client, records_to_kinesis, stream_name)
    except Exception as e:
        raise e


def lambda_handler(event, context):
    stream_name_target = event.get('stream_name', None)
    partition_key = event.get('partition_key', None)
    records_to_kinesis = event.get('records_to_kinesis', None)

    put_response: object = put_to_kinesis_stream(stream_name_target, partition_key, records_to_kinesis)

    print('response_kinesis--->: ', put_response)

# if __name__ == "__main__":
# stream_name = 'debug4RM'
# data = 'test0701'
#
# response = put_to_kinesis_stream(stream_name, 'china_iot', data)
#
# print('response_kinesis--->: ', response)
