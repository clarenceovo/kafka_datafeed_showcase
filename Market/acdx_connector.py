from Transport.ws_transport import ws_transport as webSocket
from Transport.rest_transport import rest_transport as restClient
import logging
from Quote.quote import quote
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import time
from datetime import datetime

class acdx_connector:
    def __init__(self,conn_config:dict):
        super(acdx_connector, self).__init__()
        self._connector_code = "ACDX"
        self._ws_url = conn_config['websocket_url']
        self._api_url = conn_config['api_url']
        self.quote_dict = {}

    def ping(self):
        msg="Ping"

    def subscribe_market(self,quote_list:list):

        self.quote_list = quote_list
        self._init()
        self.subscribe_market_feed()

    def get_message(self):
        msgTmp =[]
        size = self.incoming_queue.qsize()
        if size >0:
            for x in range(0,size):
                msgTmp.append(self.incoming_queue.get())
            return msgTmp
        else:
            return []
    def subscribe_market_feed(self):
        #for item in self.quote_list:
        self.subscribe_public_feed(["ticker"], self.quote_list)

    def subscribe_public_feed(self,channels,contract_code=None):
        msg = {
            "type": "subscribe",
            "channels": channels,
            "contract_codes": contract_code,
        }
        self.outgoing_queue.put(msg)
        return msg

    def stop(self):
        self.wsChannel.stop()
        logger.info(f"Stopping Websocket")
        self.wsChannel.join()



    def get_quote_dict(self,msg_list:list):
        for item in msg_list:
            if 'data' in item.keys() and item['channel'] in ['ticker']:
                data_body = item['data']
                quote_body = data_body['quote']
                ts = datetime.strptime(data_body['logical_time'],'%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
                self.quote_dict[data_body['contract_code']] = \
                    quote("ACDX",data_body['contract_code'],float(quote_body['bid']),float(quote_body['ask']),ts,float(quote_body['bid_size']),float(quote_body['ask_size']),(float(quote_body['ask'])+float(quote_body['bid']))/2)
        return self.quote_dict

    def _init(self):
        ws =webSocket(self._connector_code,self._ws_url)
        self.wsChannel = ws
        self.outgoing_queue = ws.outgoing_queue
        self.incoming_queue = ws.incoming_queue


        ws.start()