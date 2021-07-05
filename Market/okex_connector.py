import urllib
import json
from Transport.ws_transport import ws_transport as webSocket
from Transport.rest_transport import rest_transport as restClient
from .conn_util.auth_util import generate_ftx_signature,get_ftx_timestamp
import hmac
import logging
from Quote.quote import quote
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class okex_connector:
    def __init__(self,conn_config:dict):
        super(okex_connector, self).__init__()
        self._connector_code = "OKEX"
        self._ws_url = conn_config['websocket_url']
        self._api_url = conn_config['api_url']
        self.quote_dict = {}
        #self._init()

    def subscribe_market(self,quote_list:list):
        logger.info(quote_list)
        self.quote_list = quote_list
        self._init()
        self.subscribe_market_feed()

    def _init(self):

        #create WebSocket Connection
        ws =webSocket(self._connector_code,self._ws_url)
        self.wsChannel = ws
        self.outgoing_queue = ws.outgoing_queue
        self.incoming_queue = ws.incoming_queue
        ws.start()

    def subscribe_market_feed(self):
        logger.info(self.quote_list)
        for item in self.quote_list:
            quote_type , asset_code = item.split('_')
            self.wsChannel.outgoing_queue.put({"op": "subscribe", "args": [f"{quote_type}/ticker:{asset_code}"]})
    def stop(self):
        self.wsChannel.stop()
        logger.info(f"Stopping Websocket")
        self.wsChannel.join()

    def get_quote(self):
        for connector in self._conn_dict.keys():
            msg_list = self._conn_dict[connector].get_message()
        logger.info(msg_list)

    def get_message(self):
        msg_tmp = []
        size = self.incoming_queue.qsize()
        for x in range(0, size):
            msg_tmp.append(self.incoming_queue.get())
        return msg_tmp

    def get_quote_dict(self,msg_list:list):
        """
         {
          "e":"bookTicker",         // event type
          "u":400900217,            // order book updateId
          "E": 1568014460893,       // event time
          "T": 1568014460891,       // transaction time
          "s":"BNBUSDT",            // symbol
          "b":"25.35190000",        // best bid price
          "B":"31.21000000",        // best bid qty
          "a":"25.36520000",        // best ask price
          "A":"40.66000000"         // best ask qty
        }"""
        for quote_feed in msg_list:
            if 'data' in quote_feed.keys():
                data = quote_feed['data']
                for item in data:
                    quote_obj = quote(self._connector_code,item['instrument_id'],item['best_bid'],item['best_ask'],datetime.strptime(item['timestamp'],'%Y-%m-%dT%H:%M:%S.%fZ').timestamp(),item['best_bid_size'],item['best_bid_size'])
                    self.quote_dict[item['instrument_id']] = quote_obj
        return self.quote_dict
    def ping(self):
        msg="Ping"
        #self.wsChannel.outgoing_queue.put_nowait("ping")