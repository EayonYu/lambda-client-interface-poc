# -*- coding: utf-8 -*-
import base64
import json
import time

import requests

TOKEN_VALIDITY = 60 * 60 * 24 * 7


def get_saas_token(sso_id, lang, app_id):
    """
    根据ssoId、lang、appId、expiredDate生成访问Saas服务的token
    :param sso_id: 海外SSO生成的每个用户的userId
    :param lang: 语言选项
    :param app_id: 不同设备端的唯一标识id
    :return: 用于访问Saas服务的accessToken
    """
    now = int(time.time())
    expiry = now + TOKEN_VALIDITY

    payload = {
        'ssoId': sso_id,
        'lang': lang,
        'appId': app_id,
        'expiredDate': str(expiry),
        'timestamp': time.time()
    }

    # String to bytes
    bytes_token = bytes(json.dumps(payload), 'utf-8')
    # encoded = json.dumps(payload).encode()
    token = base64.b64encode(bytes_token)
    SaaS_token = token.decode()
    # token = encoded.decode('utf-8')

    return SaaS_token


def lambda_handler(event, context):
    """
    方法用于
    """
    global refresh_token
    account_system_id = event.get('accountSystemId')

    username = event.get('username')
    password = event.get('password')

    # todo
    # 1
    # login sso return accountId 、refreshToken

    url = 'https://logintest.tclclouds.com/account/internalLogin'
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
        "username": username,
        "password": password,
        "client_id": 'iot-platform'
    }
    response = requests.get(url, headers=header, params=param)
    content = json.loads(response.text)
    if content.get('status') == 1:
        account_id = content.get('user').get('id')
        refresh_token = content.get('refreshtoken')
        print("account--->:", account_id)
    else:
        account_id = None

    # todo
    # 2
    # 用accountId 和account_system_id 查询platformUserId
    # http调用springboot服务

    url = 'http://localhost:8080/user/getPlatformIdForAccountId'
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
        "accountSystemId": 'sso',
        "accountId": account_id,
    }
    response = requests.get(url, headers=header, params=param)
    content = json.loads(response.text)
    platform_user_id = content.get('rt').get('platform-user-id')
    print(content.get('rt').get('platform-user-id'))

    # todo
    # 3
    # 刷新 refresh token  ,调用lambda refrshToken
    # 返回access_token和sso_refreshtoken

    # 组装返回值 data{
    #   "platformUserId":str,
    #   "access_token":str,
    #   "sso_refreshtoken":str
    # }

    # 4.获取cognitoToken和saasToken
    SaaS_access_token = get_saas_token(account_id, 'CN', "iot-platform")

    data = {
        'iot-platform-user-id': platform_user_id,
        'saasToken': SaaS_access_token,
        'refresh_token': refresh_token
    }
    print("返回的数据-----------------------------")
    print(data)

    return data


class MyTest():
    event = {
        "accountSystemId": "sso",
        "username": "yon1@163.com",
        "password": "e10adc3949ba59abbe56e057f20f883e"
    }
    context = {

    }
    lambda_handler(event, context)


if __name__ == "__main__":
    MyTest()

# EOF
