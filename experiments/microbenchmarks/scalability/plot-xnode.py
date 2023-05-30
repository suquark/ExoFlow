import json

import numpy as np
from matplotlib import pyplot as plt

from config import N_TASKS, N_PARALLEL_TASKS

N_CONTROLLERS = tuple(range(1, 17))


def _label(n: int) -> str:
    if n == 0:
        return "1 worker"
    else:
        return f"{n + 1} workers"


def plot_scalability(prefix: str):
    fig, ax = plt.subplots(figsize=(8, 6))
    x = np.arange(1, len(N_CONTROLLERS) + 1)

    _mean, _std = [], []
    for j in N_CONTROLLERS:
        with open(f"result/dag_{prefix}_{j}_2.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label="ExoF. (1 task / DAG)")

    _mean, _std = [], []
    for j in N_CONTROLLERS:
        with open(f"result/task_{prefix}_{j}_2.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label=f"ExoF. ({N_PARALLEL_TASKS} tasks / DAG)")

    _mean, _std = [], []
    for j in N_CONTROLLERS:
        # ray_{n_controllers}_{n_executors}.json
        with open(f"result/ray_dag_{prefix}_{j}_2.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label="Ray (1 task / batch)")

    _mean, _std = [], []
    for j in N_CONTROLLERS:
        # ray_{n_controllers}_{n_executors}.json
        with open(f"result/ray_task_{prefix}_{j}_2.json") as f:
            t = N_TASKS / np.array(json.load(f))
            _mean.append(np.mean(t))
            _std.append(np.std(t))
    ax.errorbar(x, _mean, _std, label=f"Ray ({N_PARALLEL_TASKS} task / batch)")

    ax.grid(which="both", axis="y", ls=":")
    ax.grid(which="both", axis="x", ls=":")

    ax.set_xticks(x, [str(t) for t in N_CONTROLLERS], rotation=0)
    y_ticks = range(0, 5001, 1000)
    y_tick_labels = [f"{y}" for y in y_ticks]
    ax.set_yticks(y_ticks, y_tick_labels)
    # ax.set_title("Workflow Scalability")
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
        [box.x0, box.y0, box.width, box.height * 0.86]
    )
    fig.savefig(f"plots/microbenchmark-{prefix}-scalability.png")
    fig.savefig(f"plots/microbenchmark-{prefix}-scalability.pdf")


if __name__ == "__main__":
    plt.rc("font", size=22, family="Times")
    plt.rc("xtick", labelsize=20)
    plt.rc("ytick", labelsize=20)
    plt.rc("legend", fontsize=20)
    plot_scalability("1node")
    plot_scalability("4node")
