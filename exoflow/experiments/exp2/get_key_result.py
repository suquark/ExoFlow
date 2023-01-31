import json
import numpy as np


def _reduce_by(origin, new):
    return f"{(1 - new / origin) * 100:.0f}%"


def _reduce_by_50_99(origin, new):
    return (
        f"{_reduce_by(origin['p50'], new['p50'])} (p50), "
        f"{_reduce_by(origin['p99'], new['p99'])} (p99)"
    )


if __name__ == "__main__":
    with open("result/result.json") as f:
        r = json.load(f)
    beldi = r["beldi-cloudwatch"]["p50"][0]
    exoflow = r["workflow-server"]["p50"][0]
    print(f"ExoFlow is {_reduce_by(beldi, exoflow)} lower latency than Beldi.")

    beldi_reserve = r["beldi-cloudwatch-reserve"]
    no_WAL = r["workflow-server-reserve"]["reserve_serial"]
    parallel = r["workflow-server-reserve"]["reserve"]
    w_async = r["workflow-server-reserve"]["reserve_overlapckpt"]
    # no_async = r["workflow-server-reserve"]["reserve_nooverlapckpt"],
    print(
        f"beldi_reserve vs no_WAL: reduce by {_reduce_by_50_99(beldi_reserve, no_WAL)}"
    )
    print(f"no_WAL vs parallel: reduce by {_reduce_by_50_99(no_WAL, parallel)}")
    print(f"parallel vs async: reduce by {_reduce_by_50_99(parallel, w_async)}")

    # failure
    exoflow_p99 = np.array(r["workflow-server"]["p99"])
    exoflow_failure_p99 = np.array(r["workflow-server-failure"]["p99"])
    print((np.array(exoflow_failure_p99 / exoflow_p99) - 1.0) * 100)
