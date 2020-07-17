# coding:utf-8
# file: hola4Test.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/5/9 下午5:53
"""
description:
"""

import json
from TCLconfigs.param_utils import Param, ParamUtils

from responseUtil import Response, Code, ParamError, api_gateway_response

print('Loading function')


#

def lambda_handler(event, context):
    try:

        if not ParamUtils.all(event, context):
            raise ParamError
        param = Param(event)
        print(param)

        platformUserId = param.authorizer.get("platformUserId")

        # requestContext = event.get("requestContext")
        # print("requestContext:", requestContext)

        # authorizer = requestContext.get("authorizer")
        # print("!!! authorizer ", authorizer)

        # platformUserId = authorizer.get("platformUserId")
        print("platformUserId---->!!!:", platformUserId)

        # # print("value1 = " + event['key1'])
        # # print("value2 = " + event['key2'])
        # # print("value3 = " + event['key3'])
        data = {
            'rt': 'CORRECT RESULT'
        }
        print(type(data))

        json_data_str = json.dumps(data)
        print(type(json_data_str))
        print(json_data_str)
    except Exception as e:
        raise e
    response = Response(Code.SUCCESS)
    print(response)
    print(type(response))
    print("type", type(response.json()))

    return api_gateway_response(response)  # Echo back the first key value
    # raise Exception('Something went wrong')


# 测试
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
                "platformUserId": "platform-user-id-52",
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
