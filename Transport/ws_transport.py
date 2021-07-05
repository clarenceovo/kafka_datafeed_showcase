import logging
import threading
import json
import os
import sys
import ssl
from queue import Queue as queue
import websocket as ws
try:
    import thread
except ImportError:
    import _thread as thread
import zlib
import multiprocessing
import time
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
class ws_transport(threading.Thread):

    def __init__(self,exchange,wsUrl):
        logger.info(exchange+" websocket is setup")
        super(ws_transport, self).__init__()
        self.incoming_queue = queue()
        self.outgoing_queue = queue()
        self.exchange = exchange
        self.transportType = "websocket"
        self.wsUrl = wsUrl
        self.webSocket = None


    def on_message(self,ws,message):
        if (isinstance(message,bytes)): #for OKEX byte decompress message
            message = zlib.decompress(message,-zlib.MAX_WBITS)
            message = message.decode('utf-8')
        self.incoming_queue.put_nowait(self.messageLoader(message))


    def on_error(self,ws,message):
        logger.info(self.exchange+" Websocket:"+":"+str(message))
        logger.info(f'{self.exchange} websocket is restarting')
        sys.exit(0)

        #logger.info(f'{self.exchange} websocket is reconnecting')
        #self.run()

    def on_open(self,ws):
        def run(*args):
            while True:
                try:
                    while not self.outgoing_queue.empty():
                        time.sleep(0.5)
                        msg = self.outgoing_queue.get()
                        logger.info("Sending out")
                        logger.info(msg)
                        ws.send(self.messsagePraser(msg))
                        self.outgoing_queue.task_done()
                    time.sleep(0.5)
                except Exception as e:
                    logger.info(e)
                    sys.exit(0)
        thread.start_new_thread(run, ())



    def on_close(self,ws,message):
        logger.info(message)
        logger.info(self.exchange+" websocket is closed.")
        sys.exit(0)


    def stop(self):
        logger.info("Stopping Websocket:")
        self.webChannel.keep_running=False

    def run(self):
        self.webChannel = ws.WebSocketApp(self.wsUrl,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.webChannel.on_open = self.on_open(self.webChannel)
        self.webChannel.run_forever({"cert_reqs":ssl.CERT_NONE}) #sslopt={"cert_reqs":ssl.CERT_NONE}


    def messsagePraser(self,msg):
        return json.dumps(msg)


    def messageLoader(self,msg):
        return json.loads(msg)
