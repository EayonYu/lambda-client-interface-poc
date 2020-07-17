# -*- coding: utf-8 -*-
import base64
from Crypto.Cipher import AES
import json
import time
import jwt

from urllib import parse

# AES_SECRET_KEY = 'n4vKsAx7vsOSy73u'  # 此处16|24|32个字符
AES_SECRET_KEY = '1dhLz08yCmeSpyu9'
IV = "1234567890abcdef"
# cryptography

# padding算法

BS = len(AES_SECRET_KEY)


def pad(s):
    return s + (BS - len(s) % BS) * chr(BS - len(s) % BS)


def unpad(s):
    return s[0:-ord(s[-1:])]


# class AES_ENCRYPT(object):
#
#     def __init__(self):
#         self.key = AES_SECRET_KEY
#         self.mode = AES.MODE_CBC
#
#     # 加密函数
#
#     def encrypt(self, text):
#         cryptor = AES.new(self.key.encode("utf8"),
#                           self.mode, IV.encode("utf8"))
#         self.ciphertext = cryptor.encrypt(bytes(pad(text), encoding="utf8"))
#         # AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题，使用base64编码
#         return base64.b64encode(self.ciphertext)
#
#     # 解密函数
#
#     def decrypt(self, text):
#         decode = base64.b64decode(text)
#         cryptor = AES.new(self.key.encode("utf8"),
#                           self.mode, IV.encode("utf8"))
#         plain_text = cryptor.decrypt(decode)
#         return unpad(plain_text)


def encode123():
    private_key = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEILMiyJf60M1wAOtCv7suAeeXpSI1QD83E2f2sjCIYPJ4oAoGCCqGSM49\nAwEHoUQDQgAEDAXORCsAlA7+1BLEswb4QsxXsWQ1eBTxfygjmy0vv5zwKJECCnBJ\nZMj4Cf/cZckph77F/Qz2Suik90oPxMwRVw==\n-----END EC PRIVATE KEY-----\n"

    now = int(time.time())
    expiry = now + 15 * 60

    payload = {
        'ssoId': "123",
        'appId': "123",
        'expiredDate': 12345
    }

    # encoded = jwt.encode(payload, private_key, algorithm='ES256')
    encoded = jwt.encode(payload, 'ZraRHXGCrUhtgWkS', algorithm='HS256')
    token = encoded.decode('utf-8')
    return token


def decode123(token):
    publicKey = 'ZraRHXGCrUhtgWkS'
    payload = jwt.decode(token, publicKey, algorithms=['HS256'])

    # publicKey = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEDAXORCsAlA7+1BLEswb4QsxXsWQ1\neBTxfygjmy0vv5zwKJECCnBJZMj4Cf/cZckph77F/Qz2Suik90oPxMwRVw==\n-----END PUBLIC KEY-----\n"
    # publicKey = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEi/HU73eG3zRx9Vcy6dU7nDRGbQTu\nbH6I88ObUpJNQadxhjPKy1QF89FT0ya/dppMddqopAcIIVVSWS4pKMW6qQ==\n-----END PUBLIC KEY-----\n"
    #
    # payload = jwt.decode(token, publicKey, algorithms=['ES256'])

    return payload


if __name__ == '__main__':
    # aes_encrypt = AES_ENCRYPT()
    # my_email = json.dumps({
    #     'sso_id':'123456',
    #     'client_id':'1asdfergwqefegergewfwdw'
    # })
    # e = aes_encrypt.encrypt(my_email)
    # d = aes_encrypt.decrypt(e)

    # print(my_email)
    # print(e)

    # d = aes_encrypt.decrypt("fwsJCPI86uCHhAAfcalfbg==")
    # print(d)
    # print(json.loads(d))

    jwt_token = encode123()
    print('---------------------')
    print(jwt_token)

    payload = decode123(jwt_token)
    print('---------------------')
    print(payload)

    # jwt_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJjb250ZW50IjoiWnM2NXBZdGJwS0NLNk1LTTE4bmpoamp4TERha3BkOUtIcTQ4WFJsWU9YYWdXTjk1Vk5rb05lbmdJcXBnZDRzbUZiQTNxSkNrNVpMa1xuTm94ZFhta2x6WEhqQS9BZ2tDb3hwb2tZbEZMYlNJeEFPV25SdktZM3k5MzY5d2RWVVExRyJ9.cquae-2OKfqQO8cLEi9I6m9pgxn7LbMIKhnZ4AHVUVm9BSx_DHQJUPpAmNA4twmWMZ7yuUW0fhODr8QAoc1gVQ'
    # payload = decode123(jwt_token)
    # print(payload)

    # content = payload.get('content')
    # print(content)
    # # aes decryption
    # # content_obj = json.loads(content)
    # d = aes_encrypt.decrypt('Zs65pYtbpKCK6MKM18njhjjxLDakpd9KHq48XRlYOXagWN95VNkoNengIqpgd4smFbA3qJCk5ZLkNoxdXmklzXHjA/AgkCoxpokYlFLbSIxAOWnRvKY3y9369wdVUQ1G')
    # obj = json.loads(d)
    # print(obj)
    # # t = time.time()
    # tt = int(round(time.time() * 1000))
    # print(tt)
    # # print( t > int (obj['expire_time']))
    # print( tt > int (obj['expire_time']))
    # # Determine whether client_id is the same
    # if content_obj['client_id'] != self.client_id:
    #     print( False)
    # else:
    #     # Determine the expiration time
    #     now = time.time()
    #     if now > int(content_obj['expire_time']):
    #         print( False)
    #     else:
    #         print( True)

    # 1587538887093
    # 1587542766236
    # 1587542574.6136441
