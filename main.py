import schedule
import sys
import json
from kafka import KafkaProducer
from pathlib import Path
import time
import os
from Transport.db_transport import DBConnector as db_connector
from Market.binance_connector import binance_connector as binance_conn
from Market.coinbase_connector import coinbase_connector as coinbase_conn
from Market.ftx_connector import ftx_connector as ftx_conn
from Market.acdx_connector import acdx_connector as acdx_conn
from Market.okex_connector import okex_connector as okex_conn
import logging
from Util.json_util import *
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from datetime import datetime

class market_feed:
    def __init__(self):
        self.db_conn = db_connector()
        self.subscrible_dict = {}
        self.conn_config= {}
        self.get_conn_config()
        self._conn_dict = {}
        self.heartbeat_ts = {}
        self._kafka_sender =  KafkaProducer(bootstrap_servers='')#provide the kafka server url
        if 'exchange_code' in os.environ.keys():
            exchange_topic =os.environ['exchange_code'].split(',')
            logger.info(f"Exchange Subscribed:{exchange_topic}")
            self.get_subscribed_dict(exchange_topic)
            #logger.info(exchange_topic)
            for item in exchange_topic:
                if item in ['BINANCE_FUTURE','BINANCE_SPOT']:
                    self._conn_dict[item] = binance_conn(item,self.conn_config[item])
                elif item in ['FTX']:
                    self._conn_dict[item]=ftx_conn(self.conn_config[item])
                elif item in ['COINBASE']:
                    self._conn_dict[item]=coinbase_conn(self.conn_config[item])
                elif item in ['ACDX']:
                    self._conn_dict[item]=acdx_conn(self.conn_config[item])
                elif item in ['OKEX']:
                    self._conn_dict[item]=okex_conn(self.conn_config[item])
            if 'is_alpha' in os.environ.keys():
                self.is_alpha = os.environ['is_alpha']

            else:
                self.is_alpha = False
        else:
            """
            self.get_subscribed_dict(['BINANCE_FUTURE', "BINANCE_SPOT", 'FTX', 'COINBASE'])
            self._conn_dict={
                "BINANCE_FUTURE":binance_conn("BINANCE_FUTURE",self.conn_config['BINANCE_FUTURE']),
                "BINANCE_SPOT":binance_conn("BINANCE_SPOT",self.conn_config['BINANCE_SPOT']),
                "FTX":ftx_conn(self.conn_config['FTX']),
                "COINBASE":coinbase_conn(self.conn_config['COINBASE'])
            }            
            """
            self.get_subscribed_dict(['OKEX'])
            self._conn_dict={
                "OKEX":okex_conn(self.conn_config['OKEX'])
            }
            self.is_alpha = False

        self.quote_collection = {}

    def get_conn_config(self):
        conn_path = Path("Config")
        if (os.path.lexists(os.path.join(conn_path, "config.json"))):
            self.conn_config = read_json(os.path.join(conn_path,"config.json"))
        else:
            raise IOError
            sys.exit()


    def get_subscribed_dict(self,exchange_code_list:list):
        for item in exchange_code_list:
            query = f"SELECT subscribe_item FROM market_feed_config WHERE exchange ='{item}';"
            df = self.db_conn.CUSTOM(query)

            if not df.empty:
                self.subscrible_dict[item] = ','.join(e[0] for e in df.values.tolist()).split(',')
            else:
                self.subscrible_dict[item] = []
            logger.info(self.subscrible_dict[item])
        #logger.info(self.subscrible_dict)

    def set_subscribe(self):
        logger.info("Subscribing")
        for connector in self._conn_dict.keys():
            self._conn_dict[connector].subscribe_market(self.subscrible_dict[connector])

    def ping(self):
        for connector in self._conn_dict.keys():
            self._conn_dict[connector].ping()

    def get_quote(self):
        for connector in self._conn_dict.keys():
            msg_list = self._conn_dict[connector].get_message()

            if len(msg_list) >0:
                #Set heartbeat TS
                #logger.info(msg_list)
                try:
                    incoming_quote =  self._conn_dict[connector].get_quote_dict(msg_list)
                    self.quote_collection[connector] = self._conn_dict[connector].get_quote_dict(msg_list)
                    self.heartbeat_ts[connector] = datetime.utcnow().timestamp()


                except Exception as e:
                    logger.error(e)
                    continue

    def _send_to_kafka(self):
        for exchange in self.quote_collection.keys():
            quote_feed = self.quote_collection[exchange]
            if quote_feed:

                for asset in quote_feed.keys():
                    if self.is_alpha:
                        self._kafka_sender.send(f'{exchange}_{asset}_QUOTE_ALPHA'.replace("/","-"),quote_feed[asset].to_json().encode('utf-8')) #alpha mode
                    else:
                        self._kafka_sender.send(f'{exchange}_{asset}_QUOTE'.replace("/", "-"),
                                                quote_feed[asset].to_json().encode('utf-8'))
    def _send_heartbeat(self):
        for item in self.heartbeat_ts.keys():
            self._kafka_sender.send(f'{item}_HEARTBEAT',json.dumps({"exchange":item,"ts":self.heartbeat_ts[item]}).encode('utf-8'))

    def _stop(self):
        for exchange in self._conn_dict.keys():
            self._conn_dict[exchange].stop()

    def health_check(self):
        current_ts = datetime.utcnow().timestamp()
        for item in self.heartbeat_ts.keys(): #feed heartbeat check
            ts = self.heartbeat_ts[item]
            delta_t = abs(ts-current_ts)
            logger.info(f'Health Check Now:{current_ts} Feed_ts:{ts} Delta-T:{round(delta_t,4)}')
            if delta_t > 60: #If the exchange feed is stale for 60 seconds
                self._stop()
                sys.exit(0)
                raise Exception

    def run(self):
        self.set_subscribe()

        schedule.every(2).seconds.do(self.ping)
        schedule.every(0.0001).seconds.do(self.get_quote)
        schedule.every(0.5).seconds.do(self._send_to_kafka)
        schedule.every(3).seconds.do(self._send_heartbeat)
        schedule.every(1).seconds.do(self.health_check)
        while True:
            """
            1. Get the subscribed list from the database
            2. Subscribe the channel 
            3. Update the independent quote feed in the connector
            4. Publish the quote to quote MQ
            """
            try:
                schedule.run_pending()
            except Exception as e:
                logger.error(e)
                self._stop()
                sys.exit(0)
            time.sleep(0.0001)



if __name__ == '__main__':
    main_feed = market_feed().run()
