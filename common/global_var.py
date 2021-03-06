# topology settings
BROKER_NUM = 3
REPLICA_NUM = 2
PRODUCER_NUM = 1
CONSUMER_NUM = 1

# local settings
PERSIST_THRESHOLD = 0.8

ENABLE_ROCKSDB = True

# throughput settings
produce_rate = 1100
consume_rate = produce_rate/1
MAX_RUN_TIME = 2
NODE_CHANGE_DURATION = 5

# communiction setting
MSG_SIZE = 32
MAX_MSG_CAPACITY = 1024/MSG_SIZE
KEY_SIZE = 16
WAIT_TIME = 128
NEXT_CONSUME_TIME = 1/consume_rate/2 #ms