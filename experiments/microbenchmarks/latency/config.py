POINTS = (1, 128 * 2 ** 10, 2 ** 20, 32 * 2 ** 20, 128 * 2 ** 20)  # , 2 ** 30)

AIRFLOW_SIZE_LIMIT = 50 * 2 ** 20  # ~50 MB
STEPFUNCTION_LIMIT = 256 * 2 ** 10  # 256 KB
LAMBDAS_LIMIT = 6 * 2 ** 20  # 6 MB

N_AIRFLOW_DATA = len([p for p in POINTS if p < AIRFLOW_SIZE_LIMIT])
N_STEPFUNCTION_DATA = len([p for p in POINTS if p < STEPFUNCTION_LIMIT])
N_LAMBDAS_DATA = len([p for p in POINTS if p < LAMBDAS_LIMIT])

CHECKPOINT_MAP = {"sync": True, "async": "async", "skip": False}
WORKFLOW_LAMBDA_PREFIX = "workflow_lambda_"
WORKFLOW_RAY_PREFIX = "workflow_ray_"
LAMBDAS_PREFIX = "microbenchmark-dev-"
