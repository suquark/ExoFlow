import json
import numpy as np

if __name__ == "__main__":
    with open("result/airflow_8.json") as f:
        airflow = np.mean(json.load(f))
    with open("result/workflow_skip_8.json") as f:
        exoflow = np.mean(json.load(f))
    print(
        f"ExoFlow is {airflow / exoflow:.2f}x faster "
        f"than Apache Airflow for data processing"
    )
