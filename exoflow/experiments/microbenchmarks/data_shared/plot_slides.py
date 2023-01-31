import json

import numpy as np
from matplotlib import pyplot as plt
from matplotlib import cm

LABELS_MAP = {
    "Airflow": "airflow",
    # "ExoF. + NoSHM": "workflow_no_share",
    "ExoFlow + SyncCkpt": "workflow_sync",
    "ExoFlow + NoCkpt": "workflow_skip",
    "Native Spark": "spark",
}


def plot():
    fig, ax = plt.subplots(figsize=(10, 6))
    labels = [1, 2, 4, 8]
    POINTS = np.array([1, 2, 3, 4])
    patterns = ["x" * 4, "\\" * 4, "/" * 4, None]
    colors = ["w", "w", "w", cm.tab10(3)]

    width = 0.15  # the width of the bars
    n = len(LABELS_MAP)
    offsets = (np.arange(n) - (n - 1) / 2) * (width + 0.027)
    cats = list(LABELS_MAP.keys())

    def _get_data(index: int):
        mean = []
        std = []
        for i in POINTS:
            with open(f"result/{LABELS_MAP[cats[index]]}_{i}.json") as f:
                r = json.load(f)
            mean.append(np.mean(r))
            std.append(np.std(r))

        return {
            "width": width,
            "label": cats[index],
            "height": mean,
            "yerr": (np.zeros_like(std), std),
            "color": colors[index],
            "hatch": patterns[index],
            "linewidth": 1.5,
            "edgecolor": cm.tab10(index),
            "error_kw": dict(ecolor=cm.tab10(index), lw=1.5, capsize=0, capthick=1),
        }

    for i in range(len(cats)):
        _ = ax.bar(POINTS + offsets[i], **_get_data(i))

    ax.grid(which="both", axis="y", ls=":")
    # ax.set_yscale("log")
    ax.set_xticks(POINTS, labels, rotation=0)
    # ax.set_title("End-to-end Spark Execution Duration")
    ax.set_xlabel("Number of Consumers")
    ax.set_ylabel("Duration (s)")
    ax.set_ylim(bottom=0)

    (bars, labels) = ax.get_legend_handles_labels()
    order = [0, 3, 1, 2]
    bars = [bars[order[i]] for i in range(len(order))]
    labels = [labels[order[i]] for i in range(len(order))]

    fig.legend(
        bars,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.44, 1),
        ncol=2,
        labelspacing=0.5,
        columnspacing=1,
    )
    fig.tight_layout()
    box = ax.get_position()
    ax.set_position(
        [
            box.x0,
            box.y0 - box.height * 0.054,
            box.width,
            box.height,
        ]
    )
    fig.savefig("plots/microbenchmark-data-shared-slides.png")


if __name__ == "__main__":
    plt.rc("font", size=24, family="Times")
    plt.rc("xtick", labelsize=22)
    plt.rc("ytick", labelsize=22)
    plt.rc("legend", fontsize=22)
    plt.rc("hatch", linewidth=0.7)
    plot()
