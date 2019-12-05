# topology settings
BROKER_NUM = 4
REPLICA_NUM = 2
PRODUCER_NUM = 4
CONSUMER_NUM = 2

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
WAIT_TIME = 128