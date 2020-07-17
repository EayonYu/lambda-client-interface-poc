# coding:utf-8
# file: device_state_kinesis_producer_service.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/6/29 上午11:28
"""
description:
"""
import json
import logging
import datetime

from utils import init_kinesis_client
from utils import report_doing


def put_kinesis_record(stream_name, data):
    try:
        # 设备状态的kinesis stream
        device_state_kinesis_client = init_kinesis_client(stream_name)

        records_to_kinesis = list()

        records_to_kinesis.append({
            'Data': json.dumps({
                'action': 'DevicePropertyChanged',
                'partner_id': 'china_iot',
                'resource_type': 'device_state',
                'body': data
            }),
            'PartitionKey': 'china_iot'
        })

        report_doing(device_state_kinesis_client, records_to_kinesis, stream_name)
    except Exception as e:
        raise e


def MyTest():
    device_state = {"capabilities": [{"capabilityName": "temperature_controller",
                                      "properties": {"measured_temperature": {"value": 26, "timestamp": 212312323}}},
                                     {"capabilityName": "wind_controller", "properties": {}}],
                    "deviceId": "32423422",
                    "deviceIds": ["sss1", "11121231"],
                    "deviceInfo": {"category": "AC", "deviceName": "ac11", "deviceType": "EEREW-DM",
                                   "mac": "NLN:NKJ:MNKN:NKN", "model": "", "tslId": "22231"},
                    "reachability": {"value": "online", "updated_at": 12121212312}}
    print(type(device_state))
    put_kinesis_record('debug4RM', device_state)


if __name__ == "__main__":
    MyTest()
