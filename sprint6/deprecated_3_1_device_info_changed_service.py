# coding:utf-8
# file: dao_test.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/7/10 上午11:30
"""
description: 只是连接了下数据库，其他都没做
"""
import json
import sys

import pymysql

import os

# rds_user = os.environ.get('RDS_USER')
rds_user = "admin"
# rds_pwd = os.environ.get('RDS_PASSWORD')
rds_pwd = "12345678(0"


def init_rds_mysql(name=None):
    """
    initial aws rds mysql connection

    if mysql connection is not used before,we connect mysql at first time;
    else we try to reuse mysql connection.

    :param name: use which database
    :return: mysql connection
    """
    # connect to MySQL
    try:
        mysql_config = None
        mysql_conn = None
        # if init_rds_mysql() is never used before, mysql_config will be None,so we should check it and init it first
        if mysql_config is None:
            # get mysql config
            if name is None:
                mysql_config = {"host": "resource-manager-poc-s.cwjrhsqmkcjh.rds.cn-north-1.amazonaws.com.cn",
                                # {"host": "localhost",
                                "port": 3306,
                                "user": rds_user, "password": rds_pwd, "database": "db_resource_manager",
                                "connect_timeout": 5}

        # if init_rds_mysql() is never used before, mysql_conn will be None,
        # so if mysql_conn is None,we should check it and connect mysql first
        if mysql_conn is None:
            # create mysql connection
            # ** 是传参的一种方式
            mysql_conn = pymysql.connect(**mysql_config)
        else:
            # else mysql_conn is used before but timeout or disconnected,we should check it and try to reconnect
            mysql_conn.ping(True)

        return mysql_conn
    except pymysql.MySQLError as e:
        # set mysql_config and mysql_conn to None
        mysql_config = None
        mysql_conn = None
        sys.exit()


# init mysql connection,connect放在外部做成全局变量,这样lamdba生命周期里只会连接一次
global_mysql_conn = init_rds_mysql(None)


def change_device_info_batch(cursor, data_list):
    """
    方法用于 device_info_changed
    """
    sql = '''update device_info set device_name= %(deviceName)s, tsl_id= %(tslId)s, device_type=%{deviceType}s,manufacturer=%{manufacturer}s, model= %(model)s,mac=%{mac}s,
     serial_number=%{serialNo}s,firmwareVersions=%{firmwareVersions}s
     where partner_id= %(partnerId)s and partner_device_id= %(deviceId)s '''

    cursor.executemany(sql, data_list)

    return True


def lambda_handler(event, context):
    cursor = global_mysql_conn.cursor()
    try:
        data_list = event.get('payload')
        data_list_new = []
        for item in data_list:
            data = {
                "deviceId": item.get('deviceId'),
                "partnerId": item.get('partnerId'),
            }
            data.update(item.get('deviceInfo'))
            firmware_versions = item.get('deviceInfo').get("firmwareVersions")
            data.update({"firmwareVersions": json.dumps(firmware_versions)})

            data_list_new.append(data)
        print("data_list_new--->: ", data_list_new)

        # 批量修改
        change_device_info_batch(cursor, data_list_new)

        if cursor.rowcount > 0:
            print("cursor.rowcount--->: ", cursor.rowcount)
            cursor.close()
            global_mysql_conn.commit()
        return True
    except Exception as e:
        print(e)
        return False

# if __name__ == '__main__':
# data_list = [
#     {'deviceId': '2030835',
#      'deviceInfo': {'deviceName': '11空调', 'tslId': 'tsl-id-01-temp02', 'deviceType': 'DEVICE-AC',
#                     'manufacturer': 'TCL', 'model': 'AC', 'mac': '38:76:CA:44:74:28',
#                     'serialNo': '8979798hiohy986289369',
#                     'firmwareVersions': {'wifiModule': '123', 'mcu': '2331'}},
#      'partnerId': 'china_iot',
#      'messageType': 'DeviceInfoChanged.command'},
#
#     {'deviceId': '2030838',
#      'deviceInfo': {'deviceName': '11空调22', 'tslId': 'tsl-id-01-temp02', 'deviceType': 'DEVICE-AC',
#                     'manufacturer': 'TCL', 'model': 'AC', 'mac': '38:76:CA:44:74:28',
#                     'serialNo': '8979798hiohy986289369',
#                     'firmwareVersions': {'wifiModule': '123', 'mcu': '2331'}},
#      'partnerId': 'china_iot',
#      'messageType': 'DeviceInfoChanged.command'}
# ]

# lambda_handler("", "")
