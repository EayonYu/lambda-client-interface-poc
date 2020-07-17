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

rds_user = os.environ.get('RDS_USER')
rds_user = "admin"
rds_pwd = os.environ.get('RDS_PASSWORD')
rds_pwd = "12345678(0"
database = os.environ.get('RDS_DATABASE')
database = "mirror"
rds_host = os.environ.get('RDS_HOST')
rds_host = "resource-manager-poc-s.cwjrhsqmkcjh.rds.cn-north-1.amazonaws.com.cn"
rds_port = os.environ.get('RDS_PORT')
rds_port = 3306


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
                mysql_config = {"host": rds_host,
                                # {"host": "localhost",
                                "port": int(rds_port),
                                "user": rds_user, "password": rds_pwd, "database": database,
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


def insert_device_batch(cursor, data_list):
    """
    方法用于 device_info_changed
    """
    try:
        data = data_list[0]

        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * (len(data)))
        sql = '''insert ignore into user_device(partner_device_id,partner_id, partner_user_id, relationship_type, owner, privilege) VALUES({values})
              ON DUPLICATE KEY UPDATE relationship_type=values(relationship_type),owner=values(owner),privilege=values(privilege);'''.format(
            values=values)
        params = []
        for item in data_list:
            params.append(tuple(item.values()))
        params = tuple(params)
        cursor.executemany(sql, params)
        return True
    except Exception as e:
        print("insert 错误 ", e)
        raise e
        return False


def lambda_handler(event, context):
    try:
        data_list = event.get('payload')
        data_list_new = []
        for item in data_list:
            data = {
                "deviceId": item.get('deviceId'),
                "partnerId": item.get('partnerId'),
                "userId": item.get('userId'),
                "relationshipType": item.get('relationshipType'),
                "owner": item.get('owner'),
                "privilege": item.get('privilege'),
            }

            data_list_new.append(data)
        print("data_list_new--->: ", data_list_new)
        # 批量修改
        cursor = global_mysql_conn.cursor()
        insert_device_batch(cursor, data_list_new)
        if cursor.rowcount > 0:
            print("result--->: ", cursor.rowcount)

            cursor.close()
            global_mysql_conn.commit()
        return True
    except Exception as e:
        print(e)
        return False

if __name__ == '__main__':
    data_list = [
        {'deviceId': '2030835', 'userId': '2030836', 'relationshipType': 1, 'owner': 1, 'privilege': 0,
         'partnerId': 'china_iot', 'messageType': 'DeviceListChanged.command'},
        # {'deviceId': '2030840', 'userId': '2030836', 'relationshipType': 2, 'owner': 3, 'privilege': 3,
        #  'partnerId': 'china_iot', 'messageType': 'DeviceListChanged.command'}
    ]
    event = {
        "payload": data_list
    }
    lambda_handler(event, "")
