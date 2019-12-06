#!/usr/bin/env python3

import zmq
import threading
import random
import time

from common.my_timer import RecurTimer
from common.global_var import *


class Consumer():

    def __init__(self, consumer_urls, i):
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.identity = (u"Consumer-%d" % (i)).encode('ascii')
        self.__init_metrics()

        # Get producer urls in the future
        # connect to a random broker
        # self.socket.connect(random.choice(consumer_urls))
        self.socket.connect(consumer_urls[0])

        # self.timer = RecurTimer(1/consume_rate, self.send_a_req)

    def __init_metrics(self):
        self.consumed = 0
        self.failed = 0

    def run(self):
        # self.timer.start()
        # self.send_req(10)
        start_time = time.time_ns()
        ns_between_sends = 10**9/produce_rate
        avg = 0
        last_send_time = time.time_ns()
        while self.consumed < consume_rate*MAX_RUN_TIME:
            if (time.time_ns() - last_send_time > ns_between_sends):
                self.send_req(1)
                now = time.time_ns()
                avg += (now - last_send_time)/10**6
                last_send_time = now
        print("Consumer spent", (time.time_ns()-start_time)/10**9, "on rate", consume_rate, "with latency", avg/(consume_rate*MAX_RUN_TIME))

    def send_req(self, num):
        time.time()
        for _ in range(num):
            self.socket.send(b'CONSUME')
            msg = self.socket.recv_multipart()
            # print('Consumer got', msg)
            prod_id, msg_id = msg[:2]
            del msg# consumed data
            self.consume_success(prod_id, msg_id)

    def consume_success(self, prod_id, msg_id):
        self.consumed += 1
        self.socket.send_multipart([b'CONSUMED', prod_id, msg_id])
        # print("Consume one product")
        if self.consumed > consume_rate*MAX_RUN_TIME:
            # self.timer.cancel()
            print("Consumer spent", time.time_ns()-self.start_time, "on rate", consume_rate)


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
