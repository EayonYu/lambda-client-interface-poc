# -*- coding: utf-8 -*-

"""
用于解析并校验accessToken合法性的权限校验函数
"""

import json

import TCLconfigs
import boto3
# JWT stuff
# TODO: find a way to include native cryptography library
# Need to include cryptography wheels in the Lambda layer
# Right now I am just falling back to python library (slow)
import jwt
import requests
from TCLconfigs.logger import Logger
from jwt.contrib.algorithms.py_ecdsa import ECAlgorithm

# The public key corresponding to the private key used to encode the token
PUBLIC_KEY = TCLconfigs.public_key

jwt.unregister_algorithm('ES256')
jwt.register_algorithm('ES256', ECAlgorithm(ECAlgorithm.SHA256))
# Configure logger
logger = Logger().get_logger()
# dynamodb
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TCLconfigs.dynamo_table_names('user_info'))


# def build_iam_policy(principal_id, effect, resource, context):
#     """
#     构建权限策略
#     :param principal_id: 这里使用ssoId
#     :param effect: Allow or Deny
#     :param resource: 需要访问的函数资源 methodArn
#     :param context: 传递到下一个函数的数据（ssoId、lang、appId、expired）
#     """
#     policy = {
#         'principalId': principal_id,
#         'policyDocument': {
#             'Version': '2012-10-17',
#             'Statement': [
#                 {
#                     'Action': 'execute-api:Invoke',
#                     'Effect': effect,
#                     'Resource': resource,
#                 },
#             ],
#         },
#         'context': context,
#     }
#
#     # 打印日志
#     logger.info("Authorization info:%s", json.dumps(policy))
#     return policy


def lambda_handler(event, context):

    url = "127.0.0.1:8080/catalogService/products/"
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
        "clientId": ""
    }
    data = json.dumps({
        "username": "",
        "refreshToken": ""
    })
    response = requests.post(url, headers=header, params=param, data=data)
    content = json.loads(response.text)

    return content


