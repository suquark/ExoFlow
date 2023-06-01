import json

import numpy as np
import matplotlib
from matplotlib import pyplot as plt
from matplotlib import cm

PATH_PREFIX = "/exoflow/experiments/stateful_serverless"


def plot(data):
    fig, ax = plt.subplots(figsize=(8, 6))
    X = np.linspace(100, 1000, 10)
    lw = 1.4

    ax.plot(X, data["workflow-server"]["p50"], label="ExoFlow 50p", color="green")
    ax.plot(
        X,
        data["workflow-server"]["p99"],
        linestyle="--",
        label="ExoFlow 99p",
        color="green",
        linewidth=lw,
    )

    ax.plot(
        X,
        data["workflow-server-failure"]["p50"],
        label="ExoFlow 50p w/ failure",
        color="red",
        linewidth=lw,
    )
    ax.plot(
        X,
        data["workflow-server-failure"]["p99"],
        linestyle="--",
        label="ExoFlow 99p w/ failure",
        color="red",
        linewidth=lw,
    )

    ax.plot(
        X,
        data["beldi-cloudwatch"]["p50"],
        label="Beldi 50p",
        color="blue",
        linewidth=lw,
    )
    ax.plot(
        X,
        data["beldi-cloudwatch"]["p99"],
        linestyle="--",
        label="Beldi 99p",
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
    fig.savefig(f"{PATH_PREFIX}/plots/exp2-latency.png")
    fig.savefig(f"{PATH_PREFIX}/plots/exp2-latency.pdf")


def plot_reserve(data):
    fig, ax = plt.subplots(figsize=(4.4, 6))
    patterns = ["xx", "\\\\", "//", "++", "||", ".", "*", "-", "o", "O"]
    colors = ["white", "white", "white", cm.tab10(3), "white"]

    labels = [
        "Beldi",
        "-WAL",
        "+parallel",
        # "skipckpt",
        "+async",
        "-async",
    ]

    width = 0.2  # the width of the bars
    gap = 0.1
    n = len(labels)
    offsets = (np.arange(n) - (n - 1) / 2) * (width + gap)

    heights = [
        data["beldi-cloudwatch-reserve"],
        data["workflow-server-reserve"]["reserve_serial"],
        data["workflow-server-reserve"]["reserve"],
        # data["workflow-server-reserve"]["reserve_skipckpt"],
        data["workflow-server-reserve"]["reserve_overlapckpt"],
        data["workflow-server-reserve"]["reserve_nooverlapckpt"],
    ]

    for i, h in enumerate(heights):
        _ = ax.bar(
            offsets[i],
            width=width,
            height=h["p50"],
            yerr=[[0], [h["p99"] - h["p50"]]],
            label=labels[i],
            hatch=patterns[i],
            edgecolor=cm.tab10(i),
            linewidth=1.5,
            color=colors[i],
            error_kw=dict(
                ecolor=cm.tab10(i), lw=1.5, capsize=6, capthick=1.5, barsabove=False
            ),
        )

    ax.tick_params(
        axis="x",  # changes apply to the x-axis
        which="both",  # both major and minor ticks are affected
        bottom=False,  # ticks along the bottom edge are off
        top=False,  # ticks along the top edge are off
        labelbottom=False,
    )  # labels along the bottom edge are off
    # ax.set_xticks(x, labels, rotation=0)
    # ax.set_title("Latency percentile for the reservation API")
    ax.set_xlabel("\nMethod")
    ax.set_ylabel("Latency (ms)")

    # lgd = ax.legend(
    #     loc="upper center",
    #     bbox_to_anchor=(0.5, 1.22),
    #     ncol=3,
    #     labelspacing=0.2,
    # )

    lgd = ax.legend(
        loc="upper right",
        ncol=1,
        labelspacing=0.1,
        columnspacing=0.2,
        bbox_to_anchor=(0.9, 1.02),
        handlelength=0.8,
        handletextpad=0.2,
        borderpad=0,
    )
    lgd.get_frame().set_linewidth(0.0)
    fig.tight_layout()
    fig.savefig(f"{PATH_PREFIX}/plots/exp2-reserve-latency.png", bbox_extra_artists=(lgd,))
    fig.savefig(f"{PATH_PREFIX}/plots/exp2-reserve-latency.pdf", bbox_extra_artists=(lgd,))


if __name__ == "__main__":
    with open(f"{PATH_PREFIX}/result/result.json") as f:
        result = json.load(f)
    # Use Type 1 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    plt.rc("font", size=24, family="Times New Roman")
    plt.rc("legend", fontsize=22)
    plt.rc("xtick", labelsize=22)
    plt.rc("ytick", labelsize=22)
    plot(result)
    plt.rc("legend", fontsize=20.5)
    plot_reserve(result)
