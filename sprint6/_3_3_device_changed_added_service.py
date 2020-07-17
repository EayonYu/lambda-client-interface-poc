# coding:utf-8
# file: dao_test.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/7/10 上午11:30´
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
# database = os.environ.get('RDS_DATABASE')
database = "mirror"
# database = os.environ.get('RDS_HOST')
rds_host = "resource-manager-poc-s.cwjrhsqmkcjh.rds.cn-north-1.amazonaws.com.cn"
# database = os.environ.get('RDS_PORT')
rds_port = "3306"


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


def upsert_device_batch(cursor, data_list):
    """
    方法用于 device_info_changed
    """
    try:
        data = data_list[0]

        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * (len(data)))
        sql = '''
            insert ignore into device(partner_device_id,partner_id, device_name, tsl_id, device_type, manufacturer, model, mac, serial_number,firmware_versions)
            VALUES({values})
            ON DUPLICATE KEY UPDATE 
            device_name=values(device_name),tsl_id=values(tsl_id),device_type=values(device_type),manufacturer=values(manufacturer),model=values(model)
            ,mac=values(mac),serial_number=values(serial_number),firmware_versions=values(firmware_versions)'''.format(
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
        cursor = global_mysql_conn.cursor()

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

        upsert_device_batch(cursor, data_list_new)

        # sso_id_result = cursor.fetchone()
        if cursor.rowcount > 0:
            # sso_id = sso_id_result[0]
            print("result--->: ", cursor.rowcount)

            cursor.close()
            global_mysql_conn.commit()
    except Exception as e:
        print(e)
        return False


if __name__ == '__main__':
    data_list = [
        {'deviceId': '2030835',
         'deviceInfo': {'deviceName': '我的空调0716_03', 'tslId': 'tsl-id-01-temp02', 'deviceType': 'DEVICE-AC',
                        'manufacturer': '小米1', 'model': 'AC', 'mac': '382:76:CA:44:74:28',
                        'serialNo': '8979798hiohy986289369',
                        'firmwareVersions': {'wifiModule': '88', 'mcu': '32'}},
         'partnerId': 'china_iot',
         'messageType': 'DeviceInfoChanged.command'},

    ]
    event = {
        "payload": data_list
    }
    lambda_handler(event, "")
