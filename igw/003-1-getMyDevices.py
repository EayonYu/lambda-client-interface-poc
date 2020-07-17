# file: 003-1-getMyDevices.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/5/13 下午2:19
"""
description:
"""

# -*- coding: utf-8 -*-

import requests
import json
from TCLconfigs.param_utils import Param, ParamUtils

from responseUtil import Response, Code, ParamError


def lambda_handler(event, context):
    try:

        if not ParamUtils.all(event, context):
            raise ParamError
        param = Param(event)
        print(param)

        platform_user_id = param.authorizer.get("platformUserId")

        print("platformUserId---->!!!:", platform_user_id)

        url = "http://localhost:8080/user/device/getBondDevices"
        # header = {
        #     "Content-Type": "application/json;charset=UTF-8"
        # }
        param = {
            "platformUserId": platform_user_id
        }

        response = requests.get(url, params=param)
        content = json.loads(response.text)
        data = content.get("rt")
        print("data--->!!!", data)
        response = Response(Code.SUCCESS, data)
        print('\n'.join(['%s:%s' % item for item in response.__dict__.items()]))
    except Exception as e:
        raise e

    return response  # Echo back the first key value
    # raise Exception('Something went wrong')


class MyTest():
    event = {
        "body": '{ "test": "body"}',
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
                "platformUserId": "platform-user-id-51",
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
