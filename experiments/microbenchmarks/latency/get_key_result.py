import json

if __name__ == "__main__":
    with open("result/airflow.json") as f:
        airflow = json.load(f)["step_latency_mean"][str(32 * 2 ** 20)]
    with open("result/workflow_ray_async.json") as f:
        exoflow = json.load(f)["step_latency_mean"][str(32 * 2 ** 20)]
    print(
        f"ExoFlow is {airflow / exoflow:.0f}x faster "
        f"than Apache Airflow when passing data between steps"
    )
