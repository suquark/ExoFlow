import time
import requests

import tqdm


def send_workflow_request(
    url: str, workflow_id: str, size: int, n_repeat: int, n_warmup: int = 3
):
    def _run():
        response = requests.get(
            f"{url}/{workflow_id}", {"size": size, "trigger_time": time.time()}
        )
        return response.json()

    data = []
    for _ in tqdm.trange(n_repeat + n_warmup, desc=workflow_id):
        data.append(_run())
    return data[n_warmup:]
