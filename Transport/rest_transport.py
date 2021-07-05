from requests import Request ,Session


class rest_transport:

    def __init__(self,exchange,subAccount,apiEndpoint):
        self.exchange = exchange
        self.subAccount = subAccount
        self.transportType = "rest"
        self.endpoint = apiEndpoint
        self.session = Session()
    def createRestGet(self,targetUrl):
        req = Request("Get",self.endpoint+targetUrl)
        return req

    def createRestPost(self, targetUrl,body):
        req = Request("Post", self.endpoint + targetUrl,json=body)
        return req

"""
def restGetCall(self,url):
    res = requests.get(url)
    if res.status_code != 200:
        # This means something went wrong.
        raise Exception('GET /tasks/ {}'.format(res.status_code))
    return res.json()

def resPostCall(self,url,jsonBody):
    res = requests.post(url, json=jsonBody,headers={'Content-Type':'application/json'})
    if res.status_code != 201:
        raise Exception('POST /tasks/ {}'.format(res.status_code))
    return res.json()

"""
