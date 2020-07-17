# -*- coding: utf-8 -*-
import json

import requests


def lambda_handler(event, context):
    """
    方法用于
    """
    body = event.get('body', None)

    username = body.get('username', None)
    password = body.get('password', None)

    url = 'https://logintest.tclclouds.com/account/internalLogin'
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
        "username": username,
        "password": password,
        "client_id": 'iot-platform'
    }
    data = json.dumps({

    })
    response = requests.get(url, headers=header, params=param)
    login_response_content = json.loads(response.text)
    print("login_response_content: --->", login_response_content)
    if login_response_content.get("status") == 1:
        email = login_response_content.get("user").get("email")

        if email is not None and len(email) != 0:
            detail_type = "email"
        else:
            detail_type = "mobile_phone"

        # 注册完成后刷新token
        url = 'https://s246cs56yj.execute-api.ap-southeast-1.amazonaws.com/poc/token'
        header = {
            "Content-Type": "application/json;charset=UTF-8"
        }
        param = {

        }

        data = json.dumps({
            "body": {
                "userId": login_response_content.get("user").get("id"),
                "refreshToken": login_response_content.get("loginCallBack").get("code"),
                "clientId": "iot-platform",
                "username": login_response_content.get("user").get("username"),
                "detailType": detail_type
            }
        })
        response = requests.post(url, headers=header, params=param, data=data)
        refresh_token_content = json.loads(response.text)
        print("refresh_token_content:---->", refresh_token_content)
        # res = Response(Code.SUCCESS, refresh_token_content.get("data"))
        return refresh_token_content
        # return api_gateway_response(res)
    else:
        res = {
            'statusCode': 200,
            'body': json.dumps("User Not Exist or Password Not Correct")
        }
        return res


class MyTest():
    event = {
        "body": {
            "username": "13120575590",
            "password": "e10adc3949ba59abbe56e057f20f883e"
        }
    }
    res = lambda_handler(event, "")
    print("res:--->", res)


if __name__ == "__main__":
    MyTest()

# EOF
