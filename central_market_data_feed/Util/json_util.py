import json

def read_json(filepath):
    with open(filepath) as file:
      dataObj = json.load(file)
      return dataObj

def dict_to_json(dict):
    return json.dumps(dict)