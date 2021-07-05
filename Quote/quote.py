import json
import time
class quote:
    def __init__(self,exchange,code,bid,ask,timestamp,bid_qty=None,ask_qty=None,price=None):

        self.exchage = exchange
        self.code = code
        self.best_bid = float(bid)
        self.best_ask = float(ask)
        self.timestamp =int(str(timestamp)[0:10])
        #price = float(price)
        if bid_qty and ask_qty:
            self.bid_qty = float(bid_qty)
            self.ask_qty = float(ask_qty)
        else:
            self.ask_qty = None
            self.bid_qty = None
        if price:
            self.price = round(price,8)
        else:
            self.price = float((self.best_bid +self.best_ask)/2,8)

    def to_json(self):
        return json.dumps({
            "exchange":self.exchage,
            "asset":self.code,
            "bid":self.best_bid,
            "ask":self.best_ask,
            "bid_qty":self.bid_qty,
            "ask_qty":self.ask_qty,
            "trade_price":self.price,
            "source_ts":self.timestamp,
            "ts": time.time()
        })
    def __repr__(self):
        return json.dumps({
            "exchange":self.exchage,
            "asset":self.code,
            "bid":self.best_bid,
            "ask":self.best_ask,
            "bid_qty":self.bid_qty,
            "ask_qty":self.ask_qty,
            "trade_price":self.price,
            "source_ts":self.timestamp,
            "ts": time.time()
        })
        #return f'Exchange:{self.exchage}\nAsset:{self.code}\nBest Bid:{self.best_bid}\nBest Ask:{self.best_ask}\nBid Quantity:{self.bid_qty}\nAsk Quantity:{self.ask_qty}\nTS:{self.timestamp}'
