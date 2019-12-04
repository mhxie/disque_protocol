#!/usr/bin/env python3

import zmq
import threading
import random
import string

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from common.my_timer import RecurTimer
from common.global_var import *


class Producer():

    def __init__(self, producer_urls, i):
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.identity = (u"Producer-%d" % (i)).encode('ascii')

        # Get producer urls in the future
        # connect to a random broker
        self.socket.connect(random.choice(producer_urls))

        self.timer = RecurTimer(1/produce_rate, self.send_a_msg)

    def __init_metrics(self):
        self.sent = 0
        self.failed = 0

    def run(self):
        self.timer.start()

    def send_a_msg(self):
        reply = None
        random_msg = ''.join(
            [random.choice(string.ascii_letters + string.digits) for n in xrange(MSG_SIZE)])
        while reply != b'SUCCESS':
            try:
                self.socket.send(random_msg)
                reply = self.socket.recv()
            except zmq.ContextTerminated:
                print("ZMQ context terminated.")
                return
        self.send_success()

    def send_success(self):
        self.sent += 1
        if self.sent > MAX_RUN_TIME:
            self.timer.cancel()


def producer_thread(i):
    prod_urls = ["ipc://frontend-%s.ipc" % str(i) for i in range(BROKER_NUM)]
    new_prod = Producer(prod_urls, i)
    new_prod.run()


if __name__ == '__main__':
    # create producer thread
    for i in range(PRODUCER_NUM):
        thread = threading.Thread(target=producer_thread, args=(i, ))
        thread.daemon = True
        thread.start()