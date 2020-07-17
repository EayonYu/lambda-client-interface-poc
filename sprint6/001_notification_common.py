# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/5/13 下午2:19
"""
description:
"""

# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

import json
from TCLconfigs.param_utils import Param, ParamUtils
from boto3 import client as boto3_client

from responseUtil import Response, Code, ParamError, api_gateway_cors_response

print('Notification Loading Function')
lambda_client = boto3_client('lambda')


def lambda_handler(event, context):
    try:
        if not ParamUtils.all(event, context):
            raise ParamError
        param = Param(event)
        param_body = param.body
        print('param.body--->：', type(param.body), param_body)
        body_header = param_body.get('header')
        # dict
        print("header--->: ", type(body_header), body_header)
        message_type = body_header.get('messageType')
        partner_id = body_header.get('partnerId')

        body_payload_list = param_body.get('payload')
        # list
        print("payload--->: ", type(body_payload_list), body_payload_list)

        body_payload_list_new = []

        if message_type == "DeviceDeleted.command":
            print("开始--->调用删除device的lambda")
            for device_id in body_payload_list:
                # 调用lambda 的data
                item = {
                    "deviceId": device_id
                }
                item.update({
                    "partnerId": partner_id,
                    "messageType": message_type
                })
                body_payload_list_new.append(item)
        else:
            # DeviceInfoChanged.command………………
            for device in body_payload_list:
                # 调用lambda 的data
                device.update({
                    "partnerId": partner_id,
                    "messageType": message_type
                })
            body_payload_list_new = body_payload_list

        # 看下item有没有改变
        print('body_payload_list--->type: ', type(body_payload_list), body_payload_list)
        lambda_params = {
            'stream_name': 'debug4partner_batch'
            , 'partition_key': 'china_iot'
            , 'records_to_kinesis': body_payload_list_new
        }

        to_kinesis_data_list = json.dumps(lambda_params)
        print("to_kinesis_data_list--->: ", to_kinesis_data_list)
        #     把list put to kinesis
        invoke_response = lambda_client.invoke(FunctionName="CloudPoc-IOT-Mirror-Kinesis-Device-Property-Put",
                                               InvocationType='RequestResponse', LogType='Tail',
                                               Payload=to_kinesis_data_list)
        payload_res = json.loads(invoke_response['Payload'].read().decode("utf-8"))
        print("payload_res--->", payload_res)

        # 组装返回值 start
        body_header.update({"messageType": body_header.get("messageType") + ".ack"})
        response_body = {
            "header": body_header,
            "payload": "SUCCESS"
        }

        if payload_res.get('ResponseMetadata').get('HTTPStatusCode') == 200:
            # response = Response(Code.SUCCESS, response_body)
            # return api_gateway_cors_response(response)
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps(response_body)

            }
        # 组装返回值 end

    # dumps_payload = json.dumps(payload[0])
    # put_response: object = put_to_kinesis_stream("debug4RM", "china_iot", dumps_payload)
    # print("put_response--->", put_response)

    except Exception as e:
        print("notification runtime error--->", e)
        raise e


if __name__ == "__main__":
    param_body_info = {
        "header": {
            "protocolVersion": "0.0.1",
            "messageType": "DeviceInfoChanged.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        },
        "payload": [
            {
                "deviceId": "2030835",
                "deviceInfo": {
                    "deviceName": "测试空调",
                    "tslId": "tsl-id-01-temp02",
                    "deviceType": "DEVICE-AC",
                    "manufacturer": "TCL",
                    "model": "AC",
                    "mac": "38:76:CA:44:74:28",
                    "serialNo": "8979798hiohy986289369",
                    "firmwareVersions": {
                        "wifiModule": "123",
                        "mcu": "777777777"
                    }
                }
            }
        ]
    }
    param_body_device_deleted = {
        "header": {
            "protocolVersion": "0.0.1",
            "messageType": "DeviceDeleted.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        },
        "payload": [
            "2030840", "2030835"
        ]
    }
    param_body_relationship = {
        "header": {
            "protocolVersion": "0.0.2",
            "messageType": "DeviceListChanged.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        },
        "payload": [
            {
                "deviceId": "2030835",
                "userId": "2030836",
                "relationshipType": 9,
                "owner": 0,
                "privilege": 1

            }
        ]
    }

    param_body_device_property = {
        "header": {
            "protocolVersion": "0.0.1",
            "messageType": "DevicePropertyChanged.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        }
        , "payload": [
            {
                "deviceId": "2030835",
                "reachability": {
                    "value": "online444",
                    "updated_at": 1593672252675
                },
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
        ]
    }

    param_body_device_property_v2 = {
        "header": {
            "protocolVersion": "0.0.1",
            "messageType": "DevicePropertyChanged.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        },
        "payload": [
            {
                "deviceId": "2030835",
                "temperature_controller": {
                    "measured_temperature": {
                        "value": 31,
                        "updated_at": 1593672252675
                    },
                    "mode": {
                        "value": "cold",
                        "updated_at": 1593672252677
                    }
                },
                "wind_controller": {
                    "wind_speed": {
                        "value": "middle",
                        "updated_at": 1593672252678
                    },
                    "mode": {
                        "value": "on",
                        "updated_at": 1593672252677
                    }
                },
                "reachability": {
                    "value": "offline",
                    "updated_at": 1593672252677
                },

            }
        ]
    }

    param_body_info_added = {
        "header": {
            "protocolVersion": "0.0.3",
            "messageType": "DeviceAdded.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        },
        "payload": [
            {
                "deviceId": "2030835",
                "deviceInfo": {
                    "deviceName": "zaici空调",
                    "tslId": "tsl-id-01-temp02",
                    "deviceType": "DEVICE-AC",
                    "manufacturer": "TCL",
                    "model": "AC",
                    "mac": "38:76:CA:44:74:28",
                    "serialNo": "8979798hiohy986289369",
                    "firmwareVersions": {
                        "wifiModule": "123",
                        "mcu": "00000"
                    }
                },
                "reachability": {
                    "value": "onlineceshi",
                    "updated_at": 1593672252675
                },
                "properties": {
                    "2030835:temperature_controller.measured_temperature": {
                        "value": 26,
                        "updated_at": 1593672252675
                    },
                    "2030835:temperature_controller.mode": {
                        "value": "warm",
                        "updated_at": 1312312312
                    },
                    "2030835:temperature_controller.status": {
                        "value": "on",
                        "updated_at": 1312312999
                    }
                }
            }
        ]
    }
    param_body_info_added_v2 = {
        "header": {
            "protocolVersion": "0.0.1",
            "messageType": "DeviceAdded.command",
            "partnerId": "china_iot",
            "messageId": "20da6c8e-bc2f-11ea-87ff-f40f241e6280"
        },
        "payload": [
            {
                "deviceId": "2030835",
                "temperature_controller": {
                    "measured_temperature": {
                        "value": 26,
                        "updated_at": 1593672252675
                    },
                    "mode": {
                        "value": "cold",
                        "updated_at": 1593672252677
                    },
                    "target_temperature": {
                        "value": 24,
                        "updated_at": 1593672252675
                    },
                },
                "wind_controller": {
                    "wind_speed": {
                        "value": "high",
                        "updated_at": 1593672252678
                    },
                    "mode": {
                        "value": "on",
                        "updated_at": 1593672252677
                    }
                },
                "reachability": {
                    "value": "online2",
                    "updated_at": 1593672252677
                },
                "deviceInfo": {
                    "deviceName": "测试空调",
                    "tslId": "tsl-id-01-temp02",
                    "deviceType": "DEVICE-AC",
                    "manufacturer": "TCL",
                    "model": "AC",
                    "mac": "38:76:CA:44:74:28",
                    "serialNo": "8979798hiohy986289369",
                    "firmwareVersions": {
                        "wifiModule": "wifimodule1",
                        "mcu": "mcu1"
                    }
                },
            }
        ]
    }

    dumps_param_body = json.dumps(param_body_info_added_v2)

    event = {

        # 写json ''只做包围，json 里面该怎样写还是怎样写，{ 两边千万不要加引用否则就是字符串了 }
        "body": dumps_param_body,
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
            "authorizer": {
                "accountId": "1155596075",
                "platformUserId": "600000001",
                "accountSystemId": "sso",
                "clientId": "iot-platform",
                "expired": False
            }
        },
        "queryStringParameters": {"foo": "bar"},
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "POST",
        "stageVariables": {"baz": "qux"},
        "path": "/examplepath",

    }
    context = '2'

    data = lambda_handler(event, context)
    # print('\n'.join(['%s:%s' % item for item in data.__dict__.items()]))
    print("最终的lambda返回值--->", data)
