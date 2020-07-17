# -*- coding: utf-8 -*-
import json
import datetime

import requests
import time
import logging




class MyTest():
    # url = "http://localhost:8080/user/platform/add"
    # header = {
    #     "Content-Type": "application/json;charset=UTF-8"
    # }
    # sso_id = 99242554
    # param = {
    # }
    # data = json.dumps({
    #
    #     "loginDetail": {
    #         'platformUserId': 'iot-platform-' + str(sso_id)
    #     }
    # })
    # response = requests.post(url, headers=header, params=param, data=data)
    # content = json.loads(response.text)
    # print(content)

    # 字符串转时间戳
    tss1 = '2013-10-10 23:40:00'
    # 转为时间数组
    timeArray = time.strptime(tss1, "%Y-%m-%d %H:%M:%S")
    print(timeArray)
    # timeArray可以调用tm_year等
    print(timeArray.tm_year)  # 2013
    # 转为时间戳
    timeStamp = int(time.mktime(timeArray))
    print(timeStamp)  # 1381419600

    # 时间戳转字符串
    dateArray = datetime.datetime.fromtimestamp(timeStamp)
    otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
    print(otherStyleTime)  # 2013--10--10 23:40:00




if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.debug("debug")
    logger.warning("warning")
    logger.info("info")
    MyTest()

# EOF
