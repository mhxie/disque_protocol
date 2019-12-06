#!/usr/bin/env python3

import zmq
import threading
import random
import string
import time
# import asyncio

# from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

from common.my_timer import RecurTimer
from common.global_var import *


class Producer():

    def __init__(self, broker_urls, i):
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.identity = (u"Producer-%d" % (i)).encode('ascii')
        self.__init_metrics()
        # Get producer urls in the future
        # connect to a random broker


        # self.socket.connect(random.choice(broker_urls))
        self.socket.connect(broker_urls[0])

        # self.timer = RecurTimer(1/produce_rate, self.send_a_msg)
        # asyncio.set_event_loop(asyncio.new_event_loop())
        # self.loop = ioloop.IOLoop.instance()



    def __init_metrics(self):
        self.msg_id = 0
        self.sent = 0
        self.failed = 0

    def run(self):
        # self.timer.start()
        # self.caller = ioloop.PeriodicCallback(self.send_a_msg, int(1000/produce_rate))
        # self.caller.start()
        self.send_a_msg()
        # ns_between_sends = 10**9/produce_rate
        # last_send_time = time.time_ns()
        # while self.sent < produce_rate*MAX_RUN_TIME:
        #     if (time.time_ns() - last_send_time > ns_between_sends):
        #         self.send_a_msg()
        #         last_send_time = time.time_ns()


    def send_a_msg(self):
        random_msg = ''.join(
            [random.choice(string.ascii_letters + string.digits) for n in range(MSG_SIZE)])
        random_key = ''.join(
            [random.choice(string.digits) for n in range(KEY_SIZE)])
        self.socket.send_multipart([random_key.encode('ascii'), random_msg.encode('ascii')])
        self.send_success()
    
    def send_failure(self):
        self.failed += 1

    def send_success(self):
        self.sent += 1
        self.msg_id += 1


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
