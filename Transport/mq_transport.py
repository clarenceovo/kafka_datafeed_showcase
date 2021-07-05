import time
import zmq


class mq_transport:
    def __init__(self):
        self._transport_name = "mq"