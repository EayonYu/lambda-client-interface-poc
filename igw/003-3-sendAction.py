# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/5/13 下午2:19
"""
description:
"""

# -*- coding: utf-8 -*-

import requests
import json
from TCLconfigs.param_utils import Param, ParamUtils

from responseUtil import Response, Code, ParamError, api_gateway_response


def lambda_handler(event, context):
    try:

        if not ParamUtils.all(event, context):
            raise ParamError
        param = Param(event)
        print(param)
        platform_device_id = param.body.get("platformDeviceId")
        body_data_str = param.body.get("data")
        print(body_data_str)

        platform_user_id = param.authorizer.get("platformUserId")

        print("platformUserId---->!!!:", platform_user_id)

        # 查看设备状态的url
        if platform_device_id is not None:
            print("platform_device_id:--->", platform_device_id)
        url = "http://localhost:8080/user/device/sendDeviceAction"
        # url = "http://18.140.5.181:8080/user/device/sendDeviceAction"
        header = {
            "Content-Type": "application/json;charset=UTF-8"
        }
        param = {
            #     "platformUserId": platform_user_id,
            #     "platformDeviceId": platform_device_id
        }
        data = json.dumps({
            "platformUserId": platform_user_id,
            "platformDeviceId": platform_device_id,
            "action": json.dumps(body_data_str.get("action")),
            "value": json.dumps(body_data_str.get("value"))
            # "set_simple_field",
            # "value": "{targetTemperature:24"
            #          "}"
        })

        response = requests.post(url, headers=header, params=param, data=data),

        content = json.loads(response[0].content)
        print("content--->", content)
        # if content.get("status") !=Code.SUCCESS.message:
        #     message = content.get("message")
        #     data = message
        # else:
        #     deviceStatus = content.get("rt").get("deviceStatus")
        #     data = deviceStatus
        # print("data--->!!!", data)
        response = Response(Code.SUCCESS, content)
        print('\n'.join(['%s:%s' %
                         item for item in response.__dict__.items()]))
    except Exception as e:
        raise e

    return api_gateway_response(response)  # Echo back the first key value
    # raise Exception('Something went wrong')


class MyTest():
    event = {
        "body": '{"platformDeviceId": "6000005", "data": {"action": "set_simple_field","value":{"targetTemperature":"26"}}}',
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
                "accountId": "1155595451",
                "platformUserId": "40000003",
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
    print(data)


if __name__ == "__main__":
    MyTest()

# EOF
