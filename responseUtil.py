# coding:utf-8
# file: responseUtil.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/5/9 下午6:20
"""
description:
"""
import json
from enum import Enum, unique


@unique
class Code(Enum):
    SUCCESS = (200, "SUCCESS")

    # 操作错误
    SERVER_DB_ERROR = (10000, "Operation error")
    # 系统繁忙，请重试
    SERVER_ERROR = (10001, "System is busy, please try again later")

    # 未知错误
    UNKNOWN_ERROR = (10002, "Unknown error")
    # 参数错误
    PARAMETER_ERROR = (10003, "Parameter error")

    # token 为空
    TOKEN_NULL = (10020, "empty token")
    # token 无效
    TOKEN_INVALID = (10011, "token invalid")
    # token 已过期
    TOKEN_EXPIRED = (10010, "expired token")

    REFRESH_TOKEN_NULL = (10030, "Empty refresh token")
    REFRESH_TOKEN_INVALID = (10031, "Wrong refresh token")
    REFRESH_TOKEN_EXPIRED = (10032, "Expired refresh token")

    PRODUCTKEY_NULL = (10050, "Empty productKey")

    PRODUCTKEY_INVALID = (10051, "Wrong productKey")

    PRODUCTSECRET_NULL = (10052, "Empty productSecret")
    PRODUCTSECRET_INVALID = (10053, "Wrong productSecret")

    # 二维码错误
    QR_CODE_INVALID = (10070, "QR code error")
    # 二维码过期
    QR_CODE_EXPIRED = (10071, "Expired QR code")

    MASTER_CANNT_CANCEL_SHARING = (10302, "The owner cannot cancel sharing himself")

    # 设备不存在
    DEVICE_NOT_EXIST = (10100, "The device does not exist")
    # 设备不在线
    DEVICE_OFFLINE = (10101, "Device not online")
    # 设备权限受限
    DEVICE_PERMISSION_LIMIT = (10102, "Device permissions limited")
    # 设备未被绑定
    DEVICE_NOT_BOUND = (10103, "The device is not bound")
    # 设备已被绑定
    DEVICE_ALREADY_BOUND = (10104, "The device is already bound")
    # 设备已分享
    DEVICE_ALREADY_SHARED = (10105, "Device Shared")
    # 不是网关设备
    DEVICE_NO_GATEWAY = (10106, "It's not a gateway device")
    # 设备分享用户数达到限制
    DEVICE_SHARED_TOO_MUCH = (10107, "Device sharing number is up to limit")
    # 用户绑定设备数量已达上限
    DEVICE_BOUND_LIMIT = (10108, "Device number is up to limit")
    # 用户不存在
    USER_NOT_EXIST = (10150, "Invalid username")
    # 用户已存在
    USER_ALREADY_EXIST = (10151, "User Already Exist")

    # 不是设备主人
    USER_NOT_DEVICE_MASTER = (10153, "Not the master user")
    # 绑定码无效
    BINDCODE_INVALID = (10300, "Invalid binding code")
    # 版本号错误
    VERSION_REJECTED = (10301, "Wrong Version number")

    # 认证失败
    AUTHENTICATION_FAILED = (10303, "Authentication failed")
    # JSON格式非法
    JSON_FORMAT_ILLEGAL = (10304, "Invalid JSON format")

    # 没有未读消息
    NO_READ_MESSAGE = (12001, "No unread messages")
    # 没有可删除的消息
    NO_DEL_MESSAGE = (12002, "No message to delete")

    # 获取天气数据失败
    GET_WEATHER_DATA_FAILED = (12003, "Failed to get weather data")
    # 获取 AQI 数据失败
    GET_AQI_DATA_FAILED = (12004, "Failed to get AQI data")
    # 更新天气缓存数据失败
    UPDATE_WEATHER_CACHE_FAILED = (12005, "Failed to update weather cache data")
    # 更新 AQI 缓存数据失败
    UPDATE_AQI_CACHE_FAILED = (12006, "Failed to update AQI data")

    @property
    def code(self):
        return self.value[0]

    @property
    def message(self):
        return self.value[1]


class ApiGatewayException(Exception):
    def __init__(self, code: Code, data=None):
        super().__init__()
        self.code = code
        self.data = data if data is not None else {}


class ParamError(ApiGatewayException):
    def __init__(self, code: Code, data=None):
        super().__init__(code, data)


class IotError(ApiGatewayException):
    def __init__(self, code: Code, data=None):
        super().__init__(code, data)


class Response:

    def __init__(self, code: Code, data=None):
        print("before init")

        self._code = code
        self._data = data if data is not None else {}

    @property
    def code(self) -> int:
        print("before _code.code")

        return self._code.code

    @property
    def message(self) -> str:
        print("before _code.message")
        return self._code.message

    @property
    def data(self):
        print("before _data")
        return self._data

    def dict(self) -> dict:
        print("before dict")
        return {
            'code': self._code.code,
            'message': self._code.message,
            'data': self._data
        }

    def json(self) -> str:
        print("before json")
        return json.dumps(self.dict())


def api_gateway_response(res: Response):
    return {
        'statusCode': 200,
        'body': res.json()
    }


def api_gateway_cors_response(res: Response):
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': res.json()
    }


def api_gateway_cors_response_any(res):
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': res

    }
