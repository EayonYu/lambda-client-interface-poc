# -*- coding: utf-8 -*-
import json

import requests


class MyTest():
    url = 'https://logintest.tclclouds.com/account/refreshtoken'
    header = {
        "Content-Type": "application/json;charset=UTF-8"
    }
    param = {
        "username": '13120575592',
        "refreshtoken":""
    }
    data = json.dumps({

    })
    response = requests.post(url, headers=header, params=param, data=data)
    content = json.loads(response.text)
    print(content)


if __name__ == "__main__":
    MyTest()

# EOF
