# -*- coding: utf-8 -*-
import json
import re

import requests
from TCLconfigs.param_utils import ParamUtils

from responseUtil import ParamError


def lambda_handler(event, context):
    """
    方法用于
    """

    try:
        body = event.get('body', None)

        username = body.get('username', None)
        password = body.get('password', None)

        ret = re.match(r"^1[23456789]\d{9}$", username)
        if ret:
            print("匹配成功 ,是手机用户")
            info_type = '3'
            detail_type = "mobile_phone"

        else:
            print("匹配失败，是邮箱用户")
            info_type = '2'
            detail_type = "email"

        if not ParamUtils.all(username, password):
            raise ParamError

        url = 'https://logintest.tclclouds.com/account/interRegister'
        header = {
            "Content-Type": "application/json;charset=UTF-8"
        }
        param = {
            "username": username,
            "password": password,
            "type": info_type,
            "client_id": "78393397"
        }
        data = json.dumps({

        })

        response = requests.get(url, headers=header, params=param, data=data)
        register_content = json.loads(response.text)
        print("register_content:---->", register_content)
        if register_content.get("status") == 1:
            print("sss")

            # 注册完成后刷新token
        #     url = 'https://s246cs56yj.execute-api.ap-southeast-1.amazonaws.com/poc/token'
        #     header = {
        #         "Content-Type": "application/json;charset=UTF-8"
        #     }
        #     param = {
        #
        #     }
        #
        #     data = json.dumps({
        #         "body": {
        #             "userId": register_content.get("user").get("id"),
        #             "refreshToken": register_content.get("refreshtoken"),
        #             "clientId": "iot-platform",
        #             "username": register_content.get("user").get("username"),
        #             "detailType": detail_type
        #         }
        #     })
        #     response = requests.post(url, headers=header, params=param, data=data)
        #     refresh_token_content = json.loads(response.text)
        #     print("refresh_token_content:---->", refresh_token_content)
        #     # res = Response(Code.SUCCESS, refresh_token_content.get("data"))
        #     return refresh_token_content
        #     # return api_gateway_response(res)
        # else:
        #     res = {
        #         'statusCode': 200,
        #         'body': 'User Already Exist'
        #     }
        #     return res


    except BaseException:
        print("Register Error")
        pass
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }

class MyTest():
    event = {
        "body": {
            "username": "yang10.yu@tcl.com",
            "password": "e10adc3949ba59abbe56e057f20f883e"
        }
    }
    res = lambda_handler(event, "")
    print(res)


if __name__ == "__main__":
    MyTest()

# EOF
