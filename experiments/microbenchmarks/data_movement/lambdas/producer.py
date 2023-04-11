import time


def main(event, context):
    start_time = time.time()
    return {
        "trigger_time": event["trigger_time"],
        "start_time": start_time,
        "data": "A" * event["size"],
    }
