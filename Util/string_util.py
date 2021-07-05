##parse the string to
import json
from datetime import datetime
import time

def body_to_string(body):
    return json.dumps(body, separators=(",", ":"))

def set_cash_variable(value):
        if(value):
            return round(float(value),3)
        else:
            return None

def string_to_datetime(str,format=None):
    return datetime.strptime(str,format)

def datetime_to_string(dt,format="%d/%m/%Y %H:%M"):
    if dt:
        return dt.strftime(format)
    else:
        return None

def timestamp_to_string(ts,format="%d/%m/%Y %H:%M"):
    return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")


def get_current_timestamp():
    return datetime.now().strftime("%d/%m/%Y %H:%M")