#!/usr/bin/env python3

import zmq
import threading
import random
import asyncio

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream
from sqlitedict import SqliteDict

from common.global_var import *
from consumer import consumer_thread
from producer import producer_thread

peer_topo = []

if not ENABLE_ROCKSDB:
    PERSIST_THRESHOLD = 1

class Broker(object):
    def __init__(self, urls, id):
        self.id = id
        self.peer_urls = urls['peers']
        self.queued_msgs = {}
        self.replicated_msgs = {}
        self.capacity = MAX_MSG_CAPACITY

        asyncio.set_event_loop(asyncio.new_event_loop())
        # self.loop = ioloop.IOLoop.current()
        self.__init_sockets(urls)
        self.__set_callback()
        self.loop = IOLoop.instance()

        self.rocks = SqliteDict(
            './remotedb-%d.sqlite' % (self.id), autocommit=True)
        # self.AAL = SqliteDict('./local_aal-%d.sqlite' % (self.id), autocommit=True)
        self.AAL={}
        # self.DAL = SqliteDict('./local_dal-%d.sqlite' % (self.id), autocommit=True) 
        self.DAL={}
        self.rocks.clear()
        # self.AAL.clear()
        # self.DAL.clear()
    
    def __init_sockets(self, urls):
        context = zmq.Context()
        self.frontend_sock = context.socket(zmq.ROUTER)
        self.backend_sock = context.socket(zmq.ROUTER)
        self.peerend_sock = context.socket(zmq.ROUTER)
        self.frontend_sock.bind(urls["producer"])
        self.backend_sock.bind(urls["consumer"])
        self.peerend_sock.bind(urls["self"])
        
        self.context = zmq.Context.instance()
        self.socket = context.socket(zmq.REQ)
        self.socket.identity = (u"Broker-%d" % (self.id)).encode('ascii')

    def __set_callback(self):
        self.frontend = ZMQStream(self.frontend_sock)
        self.backend = ZMQStream(self.backend_sock)
        self.peerend = ZMQStream(self.peerend_sock)

        self.frontend.on_recv(self.handle_frontend)
        self.backend.on_recv(self.handle_backend)
        self.peerend.on_recv(self.handle_peerend)

    def __lost_peer(self, url):
        pass

    def __new_peer_online(self, url):
        pass

    def __insert_add_ahead_log(self, prod_id, msg_id, dst):
        """ Implementation of add-ahead-log (AAL)
        """
        print('Thread', self.id, 'inserting AAL', prod_id, msg_id, dst)
        if prod_id not in self.AAL.keys():
            self.AAL[prod_id] = {}
        self.AAL[prod_id][msg_id] = dst

    def __remove_add_ahead_log(self, prod_id, msg_id):
        print("removing AAL", prod_id, msg_id)
        del self.AAL[prod_id][msg_id]

    def __insert_delete_ahead_log(self, prod_id, msg_id, dst):
        """ Implementation of delete-ahead-log (DAL)
        """
        print('Thread', self.id, 'inserting DAL', prod_id, msg_id, dst)
        if prod_id not in self.DAL.keys():
            self.DAL[prod_id] = {}
        self.DAL[prod_id][msg_id] = dst

    def __remove_delete_ahead_log(self, prod_id, msg_id):
        print("removing DAL", prod_id, msg_id)
        del self.DAL[prod_id][msg_id]

    def __send_replicates(self, prod_id, msg_id, payload):
        for url in self.peer_urls:
            self.socket.connect(url)
            self.__insert_add_ahead_log(prod_id, msg_id, url)
            self.socket.send_multipart([b'REPLICATE', prod_id, msg_id, payload])
            reply = self.socket.recv()
            if reply == b'REPLICATED':
                self.__remove_add_ahead_log(prod_id, msg_id)
                self.socket.disconnect(url)


    def __delete_replicates(self, prod_id, msg_id):
        """ needs an efficient way to store metadata of all replicates
        """
        for url in self.peer_urls:
            self.socket.connect(url)
            self.__insert_delete_ahead_log(prod_id, msg_id, url)
            self.socket.send_multipart([b'DELETE', prod_id, msg_id])
            reply = self.socket.recv()
            if reply == b'DELETED':
                self.__remove_delete_ahead_log(prod_id, msg_id)
                self.socket.disconnect(url)

    def __persist_msgs(self, prod_id, msg_id, msg):
        if prod_id not in self.rocks.keys():
            self.rocks[prod_id] = {}
        self.rocks[prod_id][msg_id] = msg

    def __retrieve_msgs(self, num=1):
        if len(self.rocks.keys() == 0):
            return
        for _ in range(num):
            prod_id = random.choice(list(self.rocks.keys()))
            msg_id, msg = self.rocks[prod_id].popitem()
            self.queued_msgs[prod_id][msg_id] = msg

    def __get_size_of_queues(self, replica=False):
        return sum([len(queue) for queue in self.queued_msgs.values()])

    def __enqueue_a_msg(self, prod_id, msg_id, payload, replica=False):
        if not replica:
            if self.__get_size_of_queues() <= self.capacity * PERSIST_THRESHOLD:
                if prod_id not in self.queued_msgs.keys():
                    self.queued_msgs[prod_id] = {}
                self.queued_msgs[prod_id][msg_id] = payload
            else:
                self.__persist_msgs(prod_id, msg_id, msg)  # sorta redundant
        else:
            if prod_id not in self.replicated_msgs.keys():
                self.replicated_msgs[prod_id] = {}
                self.replicated_msgs[prod_id][msg_id] = payload

    def __get_a_msg(self):
        try:
            prod_id = random.choice(list(self.queued_msgs.keys()))
            msg_id = random.choice(list(self.queued_msgs[prod_id].keys()))
            # msg_id, payload = self.queued_msgs[prod_id].popitem()
            return [prod_id, msg_id, self.queued_msgs[prod_id][msg_id]]
        except IndexError or KeyboardInterrupt:
            return [None, None, None]

    def __dequeue_a_msg(self, prod_id, msg_id, replica=False):
        if not replica:
            if self.__get_size_of_queues() == self.capacity * PERSIST_THRESHOLD:
                self.__retrieve_msgs()
            print("dequing:", self.queued_msgs[prod_id])
            return self.queued_msgs[prod_id].pop(msg_id)
        else:
            print("dequing replica:", self.replicated_msgs[prod_id])
            return self.replicated_msgs[prod_id].pop(msg_id)

    def __consume_a_msg(self, cons_id):
        prod_id, msg_id, payload = self.__get_a_msg()
        if prod_id is None:
            next_timer = threading.Timer(NEXT_CONSUME_TIME, self.__consume_a_msg, [cons_id])
            next_timer.start()
            return
        else:
            print('get_a_msg', prod_id, msg_id, payload)
        self.backend.send_multipart([cons_id, prod_id, msg_id, payload])

    def handle_frontend(self, msg):
        print(self.id, 'handling frontend:', msg)
        prod_id, _, msg_id, payload = msg[:4]
        self.__enqueue_a_msg(prod_id, msg_id, payload)
        self.__send_replicates(prod_id, msg_id, payload)

    def handle_backend(self, msg):
        print(self.id, 'handling backend:', msg)
        cons_id, command = msg[:2]
        if command == b'CONSUME':
            self.__consume_a_msg(cons_id)
        elif command == b'CONSUMED':
            prod_id, msg_id = msg[2:4]
            self.__dequeue_a_msg(prod_id, msg_id)
            self.__delete_replicates(prod_id, msg_id)

    def handle_peerend(self, msg):
        print(self.id, 'handling peerend', msg)
        broker_id, _, command = msg[:3]
        if command == b'REPLICATE':
            prod_id, msg_id, payload = msg[3:6]
            self.__enqueue_a_msg(prod_id, msg_id, payload, replica=True)
            # self.peerend.send_multipart([broker_id, b'', b'REPLICATED', prod_id, msg_id])
            self.peerend.send_multipart([broker_id, b'', b'REPLICATED'])
        elif command == b'DELETE':
            prod_id, msg_id = msg[3:5]
            self.__dequeue_a_msg(prod_id, msg_id, replica=True)
            # self.peerend.send_multipart([broker_id, b'', b'DELETED', prod_id, msg_id])
            self.peerend.send_multipart([broker_id, b'', b'DELETED'])
        # elif command == b"REPLICATED":
        #     self.__remove_add_ahead_log(prod_id, msg_id)
        # elif command == b"DELETED":
        #     self.__remove_delete_ahead_log(prod_id, msg_id)

    def run(self):
        self.loop.start()

    def stop(self):
        self.loop.stop()

def broker_thread(me):
    urls = {"producer": "ipc://frontend-%s.ipc" % str(me),
            "consumer": "ipc://backend-%s.ipc" % str(me),
            "self": "ipc://broker-%s.ipc" % str(me),
            "peers": ["ipc://broker-%s.ipc" % str(i) for i in peer_topo[me]]}
    print(urls)
    broker = Broker(urls, me)
    broker.run()


if __name__ == '__main__':
    assert(BROKER_NUM > REPLICA_NUM)
    for i in range(BROKER_NUM):
        # dsts = [j for j in range(BROKER_NUM)]
        # del dsts[i] # excude self
        # peer_topo.append(random.choices(dsts, k=REPLICA_NUM))
        peer_topo.append([(i+j+1)%BROKER_NUM for j in range(REPLICA_NUM)])


        thread = threading.Thread(target=broker_thread,
                                  args=(i,))
        thread.start()

    for i in range(PRODUCER_NUM):
        thread = threading.Thread(target=producer_thread, args=(i, ))
        thread.daemon = True
        thread.start()

    for i in range(CONSUMER_NUM):
        thread = threading.Thread(
            target=consumer_thread, args=(i, ))
        thread.daemon = True
        thread.start()
