import os
import time
from collections import namedtuple

local_test = False

# constant of the system
N_NODES = 41652230
N_PARTITIONS = 4
N_INGESTION = 2
DUMP_OUTPUT = False
DUMP_OUTPUT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "outputs"))
BATCH_UPDATE_INTERVAL = 20

if local_test:
    DEBUG = True
    N_EPOCHS = 10
    WARMUP_EPOCHS = 3
    FEED_SIZE = 200
    N_EVENT_INTERVAL = 1
    N_EPOCH_INTERVAL = 3
    N_CHECKPOINT_INTERVAL = 2
    SPARK_MEMORY = "2g"
    STORAGE = os.path.abspath("workflow_dir")
else:
    DEBUG = False
    N_EPOCHS = 350
    WARMUP_EPOCHS = 150
    FEED_SIZE = 400000  # In average, we have 17.803 bytes per line
    N_EVENT_INTERVAL = 1
    N_EPOCH_INTERVAL = 10
    N_CHECKPOINT_INTERVAL = 10
    SPARK_MEMORY = "100g"
    STORAGE = "s3://exoflow/graph_streaming"

# constant in TunkRank
# https://www.rivaliq.com/blog/good-engagement-rate-twitter/
P_RETWEET = 0.00037
THRESHOLD = 0.001

Event = namedtuple("Event", ("timestamp", "a", "b", "start_index"))
State = namedtuple("State", ("epoch_time", "partitions", "start_indices"))


def get_initial_state(init_delay: int) -> State:
    state = State(
        epoch_time=time.time_ns() + init_delay * 10 ** 9,
        partitions=[[] for _ in range(N_PARTITIONS)],
        start_indices=[0] * N_INGESTION,
    )
    return state
