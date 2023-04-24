# Plot with bigger size for slides
import json

import numpy as np
from matplotlib import pyplot as plt


def plot(data):
    fig, ax = plt.subplots(figsize=(10, 6))
    X = np.linspace(100, 1000, 10)
    lw = 1.4

    ax.plot(
        X,
        data["workflow-server"]["p99"],
        label="ExoFlow 99p",
        color="green",
        linewidth=lw,
    )

    ax.plot(
        X,
        data["workflow-server-failure"]["p99"],
        label="ExoFlow 99p w/ failure",
        color="red",
        linewidth=lw,
    )

    ax.plot(
        X,
        data["beldi-cloudwatch"]["p99"],
        label="Beldi's Workflow 99p",
        color="blue",
        linewidth=lw,
    )

    major_ticks = np.linspace(0, 1000, 11)
    minor_ticks = np.linspace(0, 1000, 21)
    ax.set_xticks(major_ticks, major=True)
    ax.set_xticks(minor_ticks, minor=True)
    major_ticks = np.linspace(0, 1600, 9)
    minor_ticks = np.linspace(0, 1600, 17)
    ax.set_yticks(major_ticks, major=True)
    ax.set_yticks(minor_ticks, minor=True)

    ax.grid(which="major", axis="x")
    ax.grid(which="minor", axis="x", alpha=0.2)
    ax.grid(which="major", axis="y")
    ax.grid(which="minor", axis="y", alpha=0.2)
    ax.set_ylim(bottom=0, top=1600)
    ax.set_xlim(left=50, right=1050)
    # ax.set_title("Response time and throughput for travel reservation service")
    lgd = ax.legend(loc="upper left", labelspacing=0.2, borderpad=0)
    lgd.get_frame().set_linewidth(0.0)
    ax.set_xlabel("Throughput (request/second)")
    ax.set_ylabel("Latency (ms)")
    fig.tight_layout()
    fig.savefig("plots/stateful_serverless-latency-slides.png")


if __name__ == "__main__":
    with open("result/result.json") as f:
        result = json.load(f)
    plt.rc("font", size=24, family="Times")
    plt.rc("legend", fontsize=22)
    plt.rc("xtick", labelsize=22)
    plt.rc("ytick", labelsize=22)
    plot(result)
