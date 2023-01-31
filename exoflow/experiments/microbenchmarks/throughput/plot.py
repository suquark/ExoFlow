import json

import numpy as np
from matplotlib import pyplot as plt

from config import N_TASKS, N_PARALLEL_TASKS

MAX_SCHEDULERS = 12


def _label(n: int) -> str:
    if n == 0:
        return "1 worker"
    else:
        return f"{n + 1} workers"


def plot_throughput():
    fig, ax = plt.subplots(figsize=(4.8, 6))
    x = np.arange(1, MAX_SCHEDULERS + 1)

    _mean, _std = [], []
    for j in range(MAX_SCHEDULERS):
        with open(f"result/dag_{j + 1}_{2}.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label="ExoF. (1 task / DAG)")

    _mean, _std = [], []
    for j in range(MAX_SCHEDULERS):
        with open(f"result/task_{j + 1}_{2}.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label=f"ExoF. ({N_PARALLEL_TASKS} tasks / DAG)")

    # _mean, _std = [], []
    # for j in range(MAX_SCHEDULERS):
    #     with open(f"result/task_{j + 1}_{1}.json") as f:
    #         t = N_TASKS / np.array(json.load(f))
    #         _mean.append(np.mean(t))
    #         _std.append(np.std(t))
    # ax.errorbar(x, _mean, _std, label=f"{N_PARALLEL_TASKS} tasks per DAG (1 worker)")

    _mean, _std = [], []
    for j in range(MAX_SCHEDULERS):
        with open(f"result/ray_{j + 1}.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label="Ray")

    # _mean, _std = [], []
    # for j in range(MAX_SCHEDULERS):
    #     with open(f"result/ray_actor_{j + 1}.json") as f:
    #         t = N_TASKS / np.array(json.load(f))
    #         _mean.append(np.mean(t))
    #         _std.append(np.std(t))
    # ax.errorbar(x, _mean, _std, label="Ray (actor)")
    #
    # _mean, _std = [], []
    # for j in range(MAX_SCHEDULERS):
    #     with open(f"result/ray_async_actor_{j + 1}.json") as f:
    #         t = N_TASKS / np.array(json.load(f))
    #         _mean.append(np.mean(t))
    #         _std.append(np.std(t))
    # ax.errorbar(x, _mean, _std, label="Ray (async actor)")

    ax.grid(which="both", axis="y", ls=":")
    ax.grid(which="both", axis="x", ls=":")

    ax.set_xticks(x, [str(t) for t in x], rotation=0)
    y_ticks = range(0, 12001, 2000)
    y_tick_labels = [f"{y // 1000}k" for y in y_ticks]
    ax.set_yticks(y_ticks, y_tick_labels)
    # ax.set_title("Workflow Throughput")
    ax.set_xlabel("Number of Controllers")
    ax.set_ylabel("Throughput (tasks/s)")
    ax.set_ylim(bottom=0)

    lgd = fig.legend(
        loc="upper center",
        bbox_to_anchor=(0.52, 1.01),
        ncol=2,
        labelspacing=0.4,
        columnspacing=0.2,
        handlelength=1.5,
        handletextpad=0.4,
    )
    lgd.get_frame().set_linewidth(0.0)

    fig.tight_layout()
    box = ax.get_position()
    ax.set_position(
        [box.x0 - box.width * 0.06, box.y0, box.width * 1.13, box.height * 0.86]
    )
    fig.savefig("plots/microbenchmark-throughput.png")
    fig.savefig("plots/microbenchmark-throughput.pdf")


if __name__ == "__main__":
    plt.rc("font", size=22, family="Times")
    plt.rc("xtick", labelsize=20)
    plt.rc("ytick", labelsize=20)
    plt.rc("legend", fontsize=20)
    plot_throughput()
