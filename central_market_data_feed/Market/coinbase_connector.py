from Transport.ws_transport import ws_transport as webSocket
from Transport.rest_transport import rest_transport as restClient
import logging
from Quote.quote import quote
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import time
from calendar import timegm

class coinbase_connector:
    def __init__(self,conn_config:dict):
        super(coinbase_connector, self).__init__()
        self._connector_code = "COINBASE"
        self._ws_url = conn_config['websocket_url']
        self._api_url = conn_config['api_url']
        self.quote_dict = {}
        #self._init()

    def subscribe_market(self,quote_list:list):
        self.quote_list = quote_list
        self._init()
        self.subscribe_market_feed()

    def ping(self):
        msg = "ping"

    def subscribe_market_feed(self):
        self.wsChannel.outgoing_queue.put({
        "type": "subscribe",
        "channels": [
            {
                "name": "ticker",
                "product_ids": [
                     ','.join(self.quote_list)
                ]
            }
        ]
        })
    def _init(self):

        #create WebSocket Connection
        ws =webSocket(self._connector_code,self._ws_url)
        self.wsChannel = ws
        self.outgoing_queue = ws.outgoing_queue
        self.incoming_queue = ws.incoming_queue
        ws.start()

    def get_quote_dict(self,msg_list:list):
        for item in msg_list:
            if item['type'] == 'ticker' and 'product_id' in item.keys():
                ts = self._get_timestamp(item['time'])
                if item['product_id'] in self.quote_dict.keys():
                    if ts < self.quote_dict[item['product_id']].timestamp:
                        continue
                self.quote_dict[item['product_id']] = quote(self._connector_code,item['product_id'],item['best_bid'],item['best_ask'],ts,None,None,(float(item['best_bid'])+float(item['best_ask']))/2)
                return self.quote_dict

    def _get_timestamp(self,time_str):
        utc_time = time.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        epoch_time = timegm(utc_time)
        return epoch_time

    def stop(self):
        self.wsChannel.stop()
        logger.info(f"Stopping Websocket")
        self.wsChannel.join()

    def get_message(self):
        msg_tmp =[]
        size = self.incoming_queue.qsize()
        for x in range(0,size):
            msg_tmp.append(self.incoming_queue.get())
        return msg_tmp

