import json

import numpy as np
from matplotlib import pyplot as plt
from matplotlib import cm

from config import POINTS, N_AIRFLOW_DATA, N_STEPFUNCTION_DATA, N_LAMBDAS_DATA


def _unit_conv(size: int) -> str:
    if size < 2 ** 10:
        return f"{size}B"
    elif size < 2 ** 20:
        return f"{size // 2 ** 10}KB"
    elif size < 2 ** 30:
        return f"{size // 2 ** 20}MB"
    else:
        return f"{size // 2 ** 30}GB"


LABELS_MAP = {
    "Airflow": "result/airflow.json",
    "AWS Std.SF-λ": "result/standard_stepfunctions.json",
    "AWS Exp.SF-λ": "result/express_stepfunctions.json",
    "ExoF.-λ SyncCkpt": "result/workflow_lambda_sync.json",
    "ExoF.-λ AsyncCkpt": "result/workflow_lambda_async.json",
    "ExoF.-λ NoCkpt": "result/workflow_lambda_skip.json",
    "ExoF.-Ray SyncCkpt": "result/workflow_ray_sync.json",
    "ExoF.-Ray AsyncCkpt": "result/workflow_ray_async.json",
    "ExoF.-Ray NoCkpt": "result/workflow_ray_skip.json",
    "Ray": "result/ray.json",
}

PATTERNS = {
    "Airflow": "",
    "AWS Std.SF-λ": "",
    "AWS Exp.SF-λ": "\\" * 5,
    "ExoF.-λ SyncCkpt": "",
    "ExoF.-λ AsyncCkpt": "x" * 5,
    "ExoF.-λ NoCkpt": "/" * 5,
    "ExoF.-Ray SyncCkpt": "",
    "ExoF.-Ray AsyncCkpt": "x" * 5,
    "ExoF.-Ray NoCkpt": "/" * 5,
    "Ray": "." * 5,
}

COLORS = {
    "Airflow": cm.tab10(0),
    "AWS Std.SF-λ": cm.tab10(1),
    "AWS Exp.SF-λ": "w",
    "ExoF.-λ SyncCkpt": cm.tab10(3),
    "ExoF.-λ AsyncCkpt": "w",
    "ExoF.-λ NoCkpt": "w",
    "ExoF.-Ray SyncCkpt": cm.tab10(6),
    "ExoF.-Ray AsyncCkpt": "w",
    "ExoF.-Ray NoCkpt": "w",
    "Ray": "w",
}


def plot_step_latency_v2():
    # linewidth = 20
    fig, ax = plt.subplots(figsize=(11, 6))
    labels = ["Trigger"] + [_unit_conv(x) for x in POINTS]

    x = np.arange(len(labels))  # the label locations
    width = 0.07  # the width of the bars
    n = len(LABELS_MAP)
    offsets = (np.arange(n) - (n - 1) / 2) * (width + 0.012)
    cats = list(LABELS_MAP.keys())

    def _get_data(index: int):
        with open(LABELS_MAP[cats[index]]) as f:
            r = json.load(f)

        def _process(d):
            d = {int(k): v for k, v in d.items()}
            # this skips datapoint of 1KB & 1 GB to save space on the plot
            d.pop(2 ** 10, None)
            d.pop(2 ** 30, None)
            d = list(d.items())
            d.sort(key=lambda x: x[0])
            return [x[1] for x in d]

        trigger_mean = _process(r["trigger_latency_mean"])[0]
        trigger_std = _process(r["trigger_latency_std"])[0]

        yerr = [trigger_std] + _process(r["step_latency_std"])

        data = {
            "width": width,
            "label": cats[index],
            "height": [trigger_mean] + _process(r["step_latency_mean"]),
            "yerr": (np.zeros_like(yerr), yerr),
            "color": COLORS[cats[index]],
            "hatch": PATTERNS[cats[index]],
            "linewidth": 1.2,
            "edgecolor": cm.tab10(index),
            "error_kw": dict(
                ecolor=cm.tab10(index), lw=1, capsize=0, capthick=1, barsabove=False
            ),
        }
        return data

    # Airflow
    x_airflow = x[: N_AIRFLOW_DATA + 1] + offsets[0]
    _ = ax.bar(x_airflow, **_get_data(0))

    # Step Functions
    for i in range(1, 3):
        _ = ax.bar(x[: N_STEPFUNCTION_DATA + 1] + offsets[i], **_get_data(i))

    # Lambdas
    for i in range(3, 6):
        _ = ax.bar(x[: N_LAMBDAS_DATA + 1] + offsets[i], **_get_data(i))

    # Workflow & Ray
    for i in range(6, 10):
        _ = ax.bar(x + offsets[i], **_get_data(i))

    ax.set_xlim(left=-0.5, right=x[-1] + 0.5)
    ax.grid(which="both", axis="y", ls="-", alpha=0.5)
    ax.set_yscale("log")
    ax.set_xticks(x, labels, rotation=0)
    # ax.set_title("Workflow Step Latency")
    ax.set_xlabel("Operation", labelpad=5)
    ax.set_ylabel("Latency (s)")

    # force rearranging the legends by inserting invisible objects
    (bars, labels) = ax.get_legend_handles_labels()
    for _ in range(2):
        bars.append(plt.Line2D((0, 0), (0, 0), linestyle="none", marker=None))
        labels.append("")

    lgd = fig.legend(
        bars,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.53, 1),
        ncol=4,
        labelspacing=0.2,
        columnspacing=0.5,
        handlelength=1,
        handletextpad=0.3,
        borderpad=0,
    )
    lgd.get_frame().set_linewidth(0.0)

    fig.tight_layout()
    box = ax.get_position()
    ax.set_position(
        [
            box.x0 - box.width * 0.03,
            box.y0 - box.height * 0.02,
            box.width * 1.06,
            box.height * 0.845,
        ]
    )
    fig.savefig("plots/microbenchmark-data-movement.png")
    fig.savefig("plots/microbenchmark-data-movement.pdf")


if __name__ == "__main__":
    plt.rc("font", size=22, family="Times")
    plt.rc("xtick", labelsize=20)
    plt.rc("ytick", labelsize=20)
    plt.rc("legend", fontsize=20)
    plt.rc("hatch", linewidth=0.7)

    plot_step_latency_v2()
