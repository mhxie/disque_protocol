#!/usr/bin/env python3

import zmq
import threading
import random

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream
from sqlitedict import SqliteDict

zmq.eventloop.ioloop.install()  # use tornado

peer_topo = []


class Broker(object):
    def __init__(self, urls):
        self.__init_sockets(urls)
        self.queued_msgs = {}
        self.capacity = MAX_MSG_CAPACITY
        self.loop = IOLoop.instance()
        self.peer_urls = urls['me']
        self.rocks = SqliteDict(
            './remotedb.sqlite', autocommit=True)
        self.AAL = SqliteDict('./local_aal.sqlite', autocommit=True)
        self.DAL = SqliteDict('./local_dal.sqlite', autocommit=True)

    def __init_sockets(self, urls):
        context = zmq.Context()
        self.frontend = ZMQStream(context.socket(zmq.ROUTER))
        self.frontend.bind(urls["producer"])
        self.backend = ZMQStream(context.socket(zmq.ROUTER))
        self.backend.bind(urls["consumer"])
        self.peerend = ZMQStream(context.socket(zmq.ROUTER))
        self.peerend.bind(urls["self"])

    def __lost_peer(self, url):
        pass

    def __new_peer_online(self, url):
        pass

    def __insert_add_ahead_log(self, key, dst):
        """ Implementation of add-ahead-log (AAL)
        """
        self.AAL[key] = dst

    def __remove_add_ahead_log(self, key):
        del self.AAL[key]

    def __insert_delete_ahead_log(self, key, dst):
        """ Implementation of delete-ahead-log (DAL)
        """
        self.DAL[key] = dst

    def __remove_delete_ahead_log(self, key):
        del self.DAL[key]

    def __send_replicates(self, msg_id, data):
        for url in self.peer_urls:
            context = zmq.Context.instance()
            socket = context.socket(zmq.REQ)

            socket.connect(url)
            sock.send_multipart(*msg)
            echo = sock.recv()

    def __delete_replicates(self, msg_id):
        """ needs an efficient way to store metadata of all replicates
        """
        for url in self.peer_urls:
            context = zmq.Context.instance()
            socket = context.socket(zmq.REQ)

            socket.connect(url)
            sock.send_multipart(b'DELETE', empty, bytes(msg_id))
            echo = sock.recv()

    def __persist_msgs(self, key, value):
        if key in self.rocks.keys():
            self.rocks[key] += [value]
        else:
            self.rocks[key] = [value]

    def __retrieve_msgs(self, num, keys=None):
        msgs = []
        if not keys:
            for i in range(num):
                self.queued_msgs.append(self.rocks.pop(
                    random.choices(self.rocks.keys())))
        else:
            for k in keys:
                self.queued_msgs.append(self.rocks.pop(key))

    def handle_frontend(self, msg):
        prod_id, empty, msg_id, data = msg[:4]
        if len(self.queued_msgs) <= self.capacity:
            if msg_id in self.queued_msgs.keys():
                self.queued_msgs[msg_id] += [msg]
            else:
                self.queued_msgs[msg_id] = [msg]
            self.__insert_add_ahead_log(msg_id)
            #
            self.__send_replicates(msg_id, data)
        else:
            self.__persist_msgs(msg_id, msg)  # sorta redundant

    def handle_backend(self, msg):
        cons_id, empty, command = msg[:3]
        if command == 'CONSUME':
            prod_id, empty, kv_pair = self.queued_msgs.pop(
                random.choice(self.queued_msgs.keys()))
            self.backend.send_multipart()

    def handle_peerend(self, msg):
        pass


def broker_thread(me):
    urls = {"producer": "ipc://frontend-%s.ipc" % str(me),
            "consumer": "ipc://backend-%s.ipc" % str(me),
            "self": "ipc://broker-%s.ipc" % str(me),
            "peers": ["ipc://broker-%s.ipc" % str(i) for i in peer_topo[me]]}
    broker = Broker(urls)
    IOLoop.instance().start()  # per-thread?


if __name__ = '__main__':
    assert(BROKER_NUM > REPLICA_NUM)
    for i in range(BROKER_NUM):
        peer_topo[i] = random.choice([for i in range(BROKER_NUM)], k=REPLICA_NUM)
        thread = threading.Thread(target=broker_thread,
                                  args=(i, ))
        thread.daemon = True
        thread.start()
