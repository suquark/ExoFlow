import time


def main(event, context):
    return {
        "trigger_time": event["trigger_time"],
        "start_time": event["start_time"],
        "end_time": time.time(),
        "size": len(event["data"]),
    }
