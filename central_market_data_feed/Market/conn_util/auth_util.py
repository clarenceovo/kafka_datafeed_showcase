import hmac
import base64
import hashlib
import time
from Util.string_util import body_to_string

def get_ftx_timestamp():
    return int(time.time() * 1000)
def generate_ftx_signature(apiSecret, ts, requestPath, httpMethod="GET", body=None):
    if body:
        body = body_to_string(body)
    else:
        body = ''

    message = str(ts) + httpMethod + requestPath + body
    try:
        secret = base64.b64decode(apiSecret)
    except base64.binascii.Error as err:
        raise IOError

    signature = hmac.new(secret, message.encode(),
                         digestmod=hashlib.sha256).digest()
    #return base64.encodestring(signature)
    return base64.encodebytes(signature)