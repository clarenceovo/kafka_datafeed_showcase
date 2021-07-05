
import urllib
import json
from Transport.ws_transport import ws_transport as webSocket
from Transport.rest_transport import rest_transport as restClient
from .conn_util.auth_util import generate_ftx_signature,get_ftx_timestamp
import hmac
import logging
from Quote.quote import quote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ftx_connector:
    def __init__(self,conn_config:dict):
        super(ftx_connector, self).__init__()
        self._connector_code = "FTX"
        self._ws_url = conn_config['websocket_url']
        self._api_url = conn_config['api_url']
        self.quote_dict = {}
        #self._init()

    def subscribe_market(self,quote_list:list):
        self.quote_list = quote_list
        self._init()
        self.subscribe_market_feed()

    def ping(self):
        payload = {"op": "ping"}
        self.wsChannel.outgoing_queue.put_nowait(payload)

    def get_quote(self):
        self.get_message()


    def _init(self):
        ws =webSocket(self._connector_code,self._ws_url)
        self.wsChannel = ws
        self.outgoing_queue = ws.outgoing_queue
        self.incoming_queue = ws.incoming_queue


        ws.start()

        #self.ftxLogin()
        #logger.info(f"Initialized FTX Connection for subaccount:{self.subAccountName}")
        #asyncio.run(self.keepPing())

    def ftxLogin(self):
        sig, ts = self.generateWSSignature()
        msg = {
            "op": "login",
                  "args": {
                    "key":self._apiKey,
                    "sign":str(sig),
                    "time":ts
                  }
                                    }

        self.wsChannel.outgoing_queue.put(msg)

    def subscribe_market_feed(self):
        for contract in self.quote_list:
            self.subscribe_public_channel("ticker",contract)

    def get_message(self):
        msg_tmp =[]
        size = self.incoming_queue.qsize()
        for x in range(0,size):
            msg_tmp.append(self.incoming_queue.get())
        return msg_tmp

    def get_quote_dict(self,msg_list:list):
        for item in msg_list:
            if item['type'] == 'update' and 'market' in item.keys():
                feed = item['data']
                if item['market'] in self.quote_dict.keys():
                    if feed['time'] < self.quote_dict[item['market']].timestamp:
                        continue
                self.quote_dict[item['market']] = quote(self._connector_code,item['market'],feed['bid'],feed['ask'],feed['time'],feed['bidSize'],feed['askSize'],price=feed['last'])
        return self.quote_dict

    def stop(self):
        self.wsChannel.stop()
        logger.info(f"Stopping Websocket")
        self.wsChannel.join()



    def subscribe_public_channel(self, channels, contractCode):
        msg = {
            "op": "subscribe",
            "channel": channels,
            "market": contractCode,
        }
        self.outgoing_queue.put(msg)
        return msg