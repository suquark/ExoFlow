"""Some configurations that differ between local and remote execution."""
local_test = False

if local_test:
    FORCE_CLEAR_DOWNLOAD_DATA = False
    PREPROCESS_FAIL_DELAY = 30
    N_EPOCHS = 10
    DATASET_FRACTION = 0.2
    DEBUG = True
    ETL_OPTIONS = {"num_cpus": 1}  # cannot be empty when applied to "@ray.remote"
    TRAIN_OPTIONS = {"num_cpus": 1}
    N_TRAINERS = 2
else:
    FORCE_CLEAR_DOWNLOAD_DATA = True
    PREPROCESS_FAIL_DELAY = 200
    N_EPOCHS = 25
    DATASET_FRACTION = 1
    DEBUG = False
    ETL_OPTIONS = {"num_cpus": 1, "resources": {"tag:data": 0.001}}
    TRAIN_OPTIONS = {"num_cpus": 1, "resources": {"tag:gpu": 0.001}}
    N_TRAINERS = 4

BATCH_SIZE = 32 // N_TRAINERS  # per-device batch size
