# -*- coding: utf-8 -*-

import base64
import json
from time import time
import jwt

import boto3

dynamoDB = boto3.resource("dynamodb")
table_user_access_info = dynamoDB.Table("user_access_info")


def query_access_token(client_id, sso_id, platform_user_id):
    """更新accessToken"""

    response = table_user_access_info.get_item(
        Key={
            # 'accountId': sso_id,
            'clientId': client_id,
            'platformUserId': platform_user_id
        },
        ProjectionExpression='accessToken'
    )

    if ('Item' in response) and ('accessToken' in response['Item']):
        return response['Item'].get('accessToken')
    return None


def build_iam_policy(principal_id, effect, resource, context):
    """
    构建权限策略
    :param principal_id: 这里使用ssoId
    :param effect: Allow or Deny￿
    :param resource: 需要访问的函数资源 methodArn
    :param context: 传递到下一个函数的数据（ssoId、lang、appId、expired）
    """
    policy = {
        'principalId': principal_id,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:Invoke',
                    'Effect': effect,
                    'Resource': resource,
                },
            ],
        },
        'context': context,
    }

    # 打印日志
    print("Authorization info:%s", json.dumps(policy))
    return policy


def lambda_handler(event, context):
    authorizer_invalid_context = {
        'code': 'Wrong token',
        'message': 'Wrong token'
    }
    try:
        # 1.获取saasToken
        saas_access_token = event.get("accessToken")
        # 注意lambda要从此处获取值
        # saas_access_token = event.get("authorizationToken")

        # payload = base64.b64decode(saas_access_token).decode()
        # payload = json.loads(payload)

        publicKey = 'ZraRHXGCrUhtgWkS'
        payload = jwt.decode(saas_access_token, publicKey, algorithms=['HS256'])

        account_id = payload.get('accountId')
        # lang = payload.get('lang')
        client_id = payload.get('clientId')
        platform_user_id = payload.get('platformUserId')

        if not account_id:
            print('sso_id is None')
            return build_iam_policy(account_id, 'Deny', event['methodArn'], authorizer_invalid_context)
        if not client_id:
            print('clientId is None')
            return build_iam_policy(account_id, 'Deny', event['methodArn'], authorizer_invalid_context)

        # todo
        #  2.检查查询SaaS_access_token是否被刷新而失效
        #  dynamoDB 查询SaaS_access_token
        #  access_token_rsp = DynamodbDao.query_access_token(client_id,sso_id)
        #  if token != access_token_rsp:
        access_token_rsp = query_access_token(client_id, account_id, platform_user_id)

        principal_id = account_id
        effect = 'Deny'
        resource = event.get("methodArn")
        authorizer_wrong_context = {
            'code': 'Wrong token',
            'message': 'Wrong token /DynamoDb not found token'
        }

        if saas_access_token != access_token_rsp:

            iam_policy = build_iam_policy(principal_id, effect, resource, authorizer_wrong_context)
            return iam_policy

        # 3.检查过期时间
        now = time()

        # expiredDate = payload['expiredDate']
        # print(expiredDate)
        # nowTime = datetime.datetime.fromtimestamp(now)
        # tokeTime = datetime.datetime.fromtimestamp(int(expiredDate))
        # print(tokeTime)
        # print(nowTime)

        expired = now > int(payload['expiredDate'])

        # 4.context将会传递给下一个lambda
        authorizer_context = {
            'accountId': account_id,
            # 'lang': lang,
            'platformUserId': platform_user_id,
            'accountSystemId': "sso",
            'clientId': client_id,
            'expired': expired
        }

        allowed = not expired

        if expired:
            authorizer_context = {
                'code': 'Expired token',
                'message': 'Expired token'
            }

    except Exception as e:
        print(e)
        allowed = False
        principal_id = ''
        authorizer_context = authorizer_invalid_context
        print("Error when doing 001Lambda_Auth_SaaS.py")
    effect = 'Allow' if allowed else 'Deny'

    resource = event['methodArn']

    # 5.创建权限策略
    iam_policy = build_iam_policy(principal_id, effect, resource, authorizer_context)
    return iam_policy


# 测试
class MyTest():
    """
    用于
    """
    event = {
        "accessToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhY2NvdW50SWQiOiIxMTU1NTk2MTA4IiwicGxhdGZvcm1Vc2VySWQiOiI0MDAwMDAwMyIsImFjY291bnRTeXN0ZW1JZCI6InNzbyIsImNsaWVudElkIjoiaW90LXBsYXRmb3JtIiwiZXhwaXJlZERhdGUiOiIxNTkwNjQ5MDQyIn0.9ZYEewBGrZhoR3jlqTXqkHYwAZqP_hMLuMqxA-MjUAw"
        ,
        "methodArn": "arn:aws:lambda:ap-southeast-1:322919161366:function:IoT-Platform-CloudPoc-AuthSaasAuthorizer-1IBS6E49VVA65"
    }
    context = ''

    lambda_handler(event, context)


if __name__ == "__main__":
    MyTest()

    # EOF
