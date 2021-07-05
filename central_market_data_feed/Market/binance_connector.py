from Transport.ws_transport import ws_transport as webSocket
from Transport.rest_transport import rest_transport as restClient
import logging
from Quote.quote import quote
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import time
class binance_connector:
    def __init__(self,name,conn_config:dict):
        super(binance_connector, self).__init__()
        self._connector_code = name
        self._ws_url = conn_config['websocket_url']
        self._api_url = conn_config['api_url']
        self.quote_dict = {}
        #self._init()

    def subscribe_market(self,quote_list:list):
        logger.info(quote_list)
        self.quote_list = quote_list
        self._init()
        self.subscribe_market_feed()

    def subscribe_market_feed(self):
        tmp = []
        for item in self.quote_list:
            tmp.append(item+'@bookTicker')

        self.wsChannel.outgoing_queue.put(
            {
                "method": "SUBSCRIBE",
                "params":tmp,
                "id": 1
            })

    def ping(self):
        msg="Ping"

    def stop(self):
        self.wsChannel.stop()
        logger.info(f"Stopping Websocket")
        self.wsChannel.join()

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
        for item in msg_list:
            count = sum([list(item.keys()).count(x) for x in [ "s","b","B","a","A"]]) #item in the feed
            if count == 5:
                if 'E' in item.keys():
                    ts = item['E']
                else:
                    ts = time.time()
                if item['s'] in self.quote_dict.keys():
                    if ts < self.quote_dict[item['s']].timestamp :
                        continue
                quote_obj = quote(self._connector_code,item['s'],item['b'],item['a'],ts,item['B'],item['A'])
                self.quote_dict[item['s']] = quote_obj
        return self.quote_dict
    def _init(self):

        #create WebSocket Connection
        ws =webSocket(self._connector_code,self._ws_url)
        self.wsChannel = ws
        self.outgoing_queue = ws.outgoing_queue
        self.incoming_queue = ws.incoming_queue
        ws.start()


    def get_message(self):
        msg_tmp = []
        size = self.incoming_queue.qsize()
        for x in range(0, size):
            msg_tmp.append(self.incoming_queue.get())
        return msg_tmp
