#!/usr/bin/env python3

import zmq
import threading
import random

from common.my_timer import RecurTimer
from common.global_var import *


class Consumer():

    def __init__(self, consumer_urls, i):
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = (u"Producer-%d" % (i)).encode('ascii')

        # Get producer urls in the future
        # connect to a random broker
        self.socket.connect(random.choice(consumer_urls))

        self.timer = RecurTimer(1/throughput, self.send_a_req)

    def __init_metrics(self):
        self.consumed = 0
        self.failed = 0

    def run(self):
        self.timer.start()

    def send_a_req(self):
        self.socket.send(b'CONSUME')
        msg = self.socket.recv()
        del msg # consumed
        self.consume_success()

    def consume_success(self):
        self.consumed += 1
        self.socket.send(b'CONSUMED')
        if self.consumed > MAX_RUN_TIME:
            self.timer.cancel()


def consumer_thread(i):
    cons_urls = ["ipc://backend-%s.ipc" % str(i) for i in range(BROKER_NUM)]
    new_cons = Consumer(cons_urls, i)
    new_cons.run()


if __name__ == '__main__':
    # create consumer thread
    for i in range(CONSUMER_NUM):
        thread = threading.Thread(
            target=consumer_thread, args=(i, ))
        thread.daemon = True
        thread.start()
