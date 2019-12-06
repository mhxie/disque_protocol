# topology settings
BROKER_NUM = 4
REPLICA_NUM = 1
PRODUCER_NUM = 1
CONSUMER_NUM = 1

# local settings
PERSIST_THRESHOLD = 0.8
MAX_MSG_CAPACITY = 10
ENABLE_ROCKSDB = True

# throughput settings
produce_rate = 50
consume_rate = 10
MAX_RUN_TIME = 30
NODE_CHANGE_DURATION = 5

# communiction setting
MSG_SIZE = 32
KEY_SIZE = 16
WAIT_TIME = 128
NEXT_CONSUME_TIME = 1/consume_rate/2 #ms