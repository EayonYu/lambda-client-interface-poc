# coding:utf-8
# file: dao_test.py
# @author: Eayon Yu (yang10.yu@tcl.com)
# @datetime:2020/7/10 上午11:30
"""
description: 只是连接了下数据库，其他都没做
"""
import sys

import pymysql

import os

rds_user = os.environ.get('RDS_USER')
rds_pwd = os.environ.get('RDS_PASSWORD')


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


def lambda_handler():
    cursor = global_mysql_conn.cursor()

    # cursor.execute(
    #     'DELETE FROM auth.oauth2_user WHERE user_id = %s AND client_id = %s', (user_id, client_id))

    cursor.execute(
        'SELECT COUNT(*) FROM PLATFORM_USER'
    )

    sso_id_result = cursor.fetchone()
    if cursor.rowcount > 0:
        sso_id = sso_id_result[0]
        print(sso_id)

    cursor.close()
    global_mysql_conn.commit()


if __name__ == '__main__':
    lambda_handler()
