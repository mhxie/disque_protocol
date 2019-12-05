#!/usr/bin/env python3

import zmq
import threading
import random

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream
from sqlitedict import SqliteDict

from common.global_var import *

# zmq.eventloop.ioloop.install()  # use tornado, for old pyzmq

peer_topo = []

if not ENABLE_ROCKSDB:
    PERSIST_THRESHOLD = 1

class Broker(object):
    def __init__(self, urls, id):
        self.id = id
        self.__init_sockets(urls)
        self.peer_urls = urls['me']
        self.queued_msgs = {}
        self.capacity = MAX_MSG_CAPACITY

        self.frontend.on_recv(self.handle_frontend)
        self.backend.on_recv(self.handle_backend)
        self.peerend.on_recv(self.handle_peerend)
        
        self.rocks = SqliteDict(
            './remotedb.sqlite', autocommit=True)
        self.AAL = SqliteDict('./local_aal.sqlite', autocommit=True)
        self.DAL = SqliteDict('./local_dal.sqlite', autocommit=True)
        
        self.loop = IOLoop.instance()

    def __init_sockets(self, urls):
        context = zmq.Context()
        self.frontend = ZMQStream(context.socket(zmq.ROUTER))
        self.frontend.bind(urls["producer"])
        self.backend = ZMQStream(context.socket(zmq.ROUTER))
        self.backend.bind(urls["consumer"])
        self.peerend = ZMQStream(context.socket(zmq.ROUTER))
        self.peerend.bind(urls["self"])
    
        self.context = zmq.Context.instance()
        self.socket = context.socket(zmq.REQ)
        self.socket.identity = (u"Broker-%d" % (i)).encode('ascii')

    def __lost_peer(self, url):
        pass

    def __new_peer_online(self, url):
        pass

    def __insert_add_ahead_log(elf, prod_id, msg_id, dst):
        """ Implementation of add-ahead-log (AAL)
        """
        if prod_id not in self.DAL.keys():
            self.AAL[prod_id] = {}
        self.AAL[prod_id][msg_id] = dst

    def __remove_add_ahead_log(self, prod_id, msg_id):
        del self.AAL[prod_id][msg_id]

    def __insert_delete_ahead_log(self, prod_id, msg_id, dst):
        """ Implementation of delete-ahead-log (DAL)
        """
        if prod_id not in self.DAL.keys():
            self.DAL[prod_id] = {}
        self.DAL[prod_id][msg_id] = dst

    def __remove_delete_ahead_log(self, prod_id, msg_id):
        del self.DAL[prod_id][msg_id]

    def __send_replicates(self, prod_id, msg_id, msg):
        for url in self.peer_urls:
            self.socket.connect(url)
            self.__insert_add_ahead_log(prod_id, msg_id, url)
            self.socket.send_multipart([b'REPLICATE']+[prod_id, msg_id]+msg)
            self.socket.disconnect()

    def __delete_replicates(self, msg_id):
        """ needs an efficient way to store metadata of all replicates
        """
        for url in self.peer_urls:
            self.socket.connect(url)
            self.__insert_delete_ahead_log(prod_id, msg_id, url)
            self.socket.send_multipart([b'DELETE']+[prod_id, msg_id])
            self.socket.disconnect()

    def __persist_msgs(self, prod_id, msg_id, msg):
        if prod_id not in self.rocks.keys():
            self.rocks[prod_id] = {}
        self.rocks[prod_id][msg_id] = msg

    def __retrieve_msgs(self, num=1):
        msgs = []
        for i in range(num):
            prod_id = random.choice(self.rocks.keys())
            msg_id = random.choice(self.rocks.keys())
            self.queued_msgs[prod_id][msg_id] += [self.rocks[prod_id].pop(msg_id)]

    def __get_size_of_queues(self):
        return sum([len(queue) for queue in self.queued_msgs.values()])

    def __enqueue_a_msg(self, prod_id, msg_id, msg):
        if self.__get_size_of_queues(self.queued_msgs) <= self.capacity * PERSIST_THRESHOLD:
            if prod_id not in self.queued_msgs.keys():
                self.queued_msgs[prod_id] = {}
            self.queued_msgs[prod_id][msg_id] = msg
            self.__insert_add_ahead_log(msg_id)
            #
            self.__send_replicates(msg)
        else:
            self.__persist_msgs(prod_id, msg_id, msg)  # sorta redundant

    def __get_a_msg(self):
        prod_id = random.choice(self.queued_msgs.keys())
        msg_id = random.choice(self.queued_msgs[prod_id].keys())
        return [prod_id, msg_id, self.queued_msgs[prod_id][msg_id]]


    def __dequeue_a_msg(self, prod_id, msg_id):
        if self.__get_size_of_queues(self.queued_msgs) == self.capacity * PERSIST_THRESHOLD:
            self.__retrieve_msgs()
        return self.queued_msgs[prod_id].pop(msg_id)

    def handle_frontend(self, msg):
        prod_id, empty, msg_id, data = msg[:4]
        self.__enqueue_a_msg(msg_id, prod_id, msg)
        self.__send_replicates()

    def handle_backend(self, msg):
        cons_id, empty, command = msg[:3]
        if command == b'CONSUME':
            prod_id, msg_id, prod_msg = self.__get_a_msg()
            self.backend.send_multipart(cons_id, b'', prod_msg)
            self.__insert_delete_ahead_log(prod_id, msg_id, cons_id)
        elif command == b'CONSUMED':
            prod_id, msg_id = msg[3:4]
            self.__dequeue_a_msg(prod_id, msg_id)
            self.__delete_replicates(prod_msg, msg_id)

    def handle_peerend(self, msg):
        broker_id, empty, command = msg[:3]
        if command == b'REPLICATE':
            prod_id, msg_id, prod_msg = msg[3:5]
            self.__enqueue_a_msg(prod_id, msg_id, prod_msg)
            self.peerend.send([broker_id, b'', b'REPLICATED', prod_id, msg_id])
        elif command == b'DELETE':
            prod_id, msg_id = msg[3:4]
            self.__dequeue_a_msg(prod_id, msg_id)
            self.peerend.send([broker_id, b'', b'DELETED', prod_id, msg_id])
        elif command == b"REPLICATED":
            self.__remove_add_ahead_log(prod_id, msg_id)
        elif command == b"DELETED":
            self.__remove_delete_ahead_log(prod_id, msg_id)

    def run(self):
        self.loop.start()

    def stop(self):
        self.loop.stop()


def broker_thread(me):
    urls = {"producer": "ipc://frontend-%s.ipc" % str(me),
            "consumer": "ipc://backend-%s.ipc" % str(me),
            "self": "ipc://broker-%s.ipc" % str(me),
            "peers": ["ipc://broker-%s.ipc" % str(i) for i in peer_topo[me]]}
    broker = Broker(urls, me)
    broker.run()


if __name__ == '__main__':
    assert(BROKER_NUM > REPLICA_NUM)
    for i in range(BROKER_NUM):
        dsts = [j for j in range(BROKER_NUM)]
        del dsts[i] # excude self
        peer_topo.append(random.choices(dsts, k=REPLICA_NUM))
        thread = threading.Thread(target=broker_thread,
                                  args=(i, ))
        thread.daemon = True
        thread.start()
