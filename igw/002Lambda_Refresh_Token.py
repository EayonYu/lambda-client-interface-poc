# -*- coding: utf-8 -*-
import datetime
import json
import time

import boto3
import jwt
import requests

# SaaSToken过期时间 7天  temporary
TOKEN_VALIDITY = 60 * 60 * 24 * 7

dynamoDB = boto3.resource('dynamodb')
table_user_access_info = dynamoDB.Table("user_access_info")


# 另外一种获取dynamoDB的方式
# client = boto3.client(
#     'dynamodb'
# )


def update_access_token(client_id, sso_id, saas_access_token, platform_user_id):
    """
    更新accessToken
    """
    # 转为字符串
    time_stamp = int(time.time())
    dateArray = datetime.datetime.fromtimestamp(time_stamp)
    time_str = dateArray.strftime("%Y-%m-%d %H:%M:%S")
    table_user_access_info.update_item(
        Key={
            # 'accountSystemId': "sso",
            # 'accountId': sso_id,
            'clientId': client_id,
            'platformUserId': platform_user_id

        }
        ,
        UpdateExpression='SET accessToken=:val1,updateTime=:val2,accountId=:val3,accountSystemId=:val4',
        ExpressionAttributeValues={
            ':val1': saas_access_token,
            ':val2': time_str,
            ':val3': sso_id,
            ':val4': "sso",
        }
    )

    # table_user_access_info.put_item(
    #     Item={
    #         'accountSystemId': "sso",
    #         'accountId': sso_id,
    #         'clientId': client_id,
    #         'platformUserId': platform_user_id,
    #         'accessToken': saas_access_token,
    #         'updateTime': time_str
    #     }
    # )


def get_saas_token(sso_id, client_id, platform_user_id):
    """
    根据ssoId、lang、appId、expiredDate生成访问Saas服务的token
    :param platform_user_id:
    :param sso_id: 海外SSO生成的每个用户的userId
    :param lang: 语言选项
    :param client_id: 不同平台的唯一标识id
    :return: 用于访问Saas服务的accessToken
    """
    now = int(time.time())
    time_stamp = now + TOKEN_VALIDITY
    # # 转为字符串
    # dateArray = datetime.datetime.fromtimestamp(time_stamp)
    # expiry = dateArray.strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        'accountId': sso_id,
        # 'lang': lang,
        'platformUserId': platform_user_id,
        'accountSystemId': "sso",
        'clientId': client_id,
        'expiredDate': str(time_stamp)
    }

    # String to bytes
    # bytes_token = bytes(json.dumps(payload), 'utf-8')
    # encoded = json.dumps(payload).encode()
    # token = base64.b64encode(bytes_token)
    # SaaS_token = token.decode()

    PRIVATE_KEY = 'ZraRHXGCrUhtgWkS'

    encoded = jwt.encode(payload, PRIVATE_KEY, algorithm='HS256')
    SaaS_token = encoded.decode('utf-8')
    return SaaS_token


def insert_sso_id(login_detail, personal_detail):
    """
    """
    url = 'http://localhost:8080/user/platform/add'
    url = 'http://18.140.5.181:8080/user/platform/add'
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
    }
    data = json.dumps({
        "loginDetail": login_detail,
        "personalDetail": personal_detail
    })

    response = requests.post(url, headers=header, params=param, data=data)
    content = json.loads(response.text)
    platform_user_id = content.get("rt").get("platform-user-id")

    print("注册平台用户successfully", "返回数据---->>>:", content)

    return platform_user_id


def validate_account_token(sso_id, refreshToken):
    """
    验证账户合法性
    """

    url = 'https://logintest.tclclouds.com/account/refreshtoken'
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
        "username": sso_id,
        "refreshToken": refreshToken
    }
    # requestParams 不需要dataBody
    data = json.dumps({
    })

    response = requests.post(url, headers=header, params=param, data=data)
    content = json.loads(response.text)
    print("sso刷新token successfully")
    # 暂时只有sso有返回值均可，因为接口目前不返回RefreshToken
    # if content.get('status') == 1:
    if content.get('status') is not None:
        return True
    else:
        print('sso账户平台登录不通过')
        return False


def lambda_handler(event, context):
    """
    方法用于 验证账户合法性 赋予iot-platform  access_token
    """

    try:
        # 1、获取参数
        body = event.get('body', None)
        sso_id = body.get('userId', None)
        username = body.get('username', None)
        refresh_token = body.get('refreshToken', None)
        client_id = body.get('clientId', None)
        detail_type = body.get('detailType', None)

        if client_id is None:
            return "PARAMETER_ERROR"

        # 2、sso验证合法性
        access_after = validate_account_token(sso_id, refresh_token)

        # 3、在数据库中插入用户数据
        # todo
        #  httprequest
        # print("插入数据,在resourcemanager里做判断有没有存在")
        # 查询resourcemanager存在不存在 return iot-platform-user-id
        # 不存在则添加，存在则不作操作
        login_detail = {
            "accountSystemId": "sso",
            "accountId": sso_id
        }
        personal_detail = {
            "detailType": detail_type,
            "detailValue": username
        }
        platform_user_id = insert_sso_id(login_detail, personal_detail)

        # 4、 获取iot-platform accessToken
        if access_after:
            SaaS_access_token = get_saas_token(sso_id, client_id, platform_user_id)

        else:
            return "ErrorEnum.SERVER_DB_ERROR"

        # todo
        # 更新accessToken
        # 如果dynamoDB里存在则更新，不存在则插入sso_id和Access_token的关系
        update_access_token(client_id, sso_id, SaaS_access_token, platform_user_id)
        # print(base64.b64decode(SaaS_access_token).decode())
        PUBLCI_KEY = 'ZraRHXGCrUhtgWkS'
        payload = jwt.decode(SaaS_access_token, PUBLCI_KEY, algorithms=['HS256'])
        print("解析出来的token内容：", payload)

        data = {
            'saasToken': SaaS_access_token,
            'refreshToken': refresh_token
        }

        print("返回的数据-----------------------------")
        print(data)

        return {"code": 0,
                "message": "SUCCESS",
                "data": data
                }

    except Exception as e:
        print(e)
        return "result_info_enum(ErrorEnum.SERVER_DB_ERROR)-end"


class MyTest():
    event = {
        "body": {
            "userId": "1155596108",
            "refreshToken": "CN_7762934634f19b9ac7560c973138415c",
            "clientId": "iot-platform",
            "username": "13120575590",
            "detailType": "mobile_phone"
        }
    }
    context = ''
    lambda_handler(event, context)


if __name__ == "__main__":
    MyTest()
# EOF
