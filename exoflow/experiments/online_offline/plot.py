from typing import List

import json
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from common import BATCH_UPDATE_INTERVAL, WARMUP_EPOCHS, N_EPOCHS

OUTPUT_DIR = "result/analyze_outputs.1103"

# N_PARTITIONS = 4
# N_INGESTION = 2
# N_EPOCHS = 500
# FEED_SIZE = 400000
# N_EVENT_INTERVAL = 1

COLORS = {
    "hybrid": "green",
    "async": "blue",
    "False": "orange",
    "True": "red",
}

LINE_STYLES = {
    "async": "-",
    "False": "-.",
    "True": "--",
}


LABELS = {
    "async": "ExoFlow AsyncCkpt",
    "False": "ExoFlow NoCkpt",
    "True": "ExoFlow SyncCkpt",
}


def read_workflow_with_checkpoint(checkpoint: str):
    for fn in Path(OUTPUT_DIR).iterdir():
        if fn.name.startswith(f"exp3-{checkpoint}"):
            with fn.open() as f:
                return json.load(f)
    assert False


def plot_checkpoint_online(checkpoint_options: List[str]):
    # # PDF
    # ax.plot(np.arange(1, 1 + len(online_data)), online_data)
    # ax.set_title("Latency of online update")
    # ax.set_xlabel("Epoch")
    # ax.set_ylabel("Latency (s)")
    # fig.savefig(f"{OUTPUT_DIR}/plots/{checkpoint}-online-pdf.png")

    # CDF
    fig, ax = plt.subplots(figsize=(7, 6))
    for checkpoint in checkpoint_options:
        data = read_workflow_with_checkpoint(checkpoint)
        online_data = np.array(data["online_update_durations"][150:]) - 10
        X = np.linspace(0, 5, 50)
        Y = np.histogram(online_data, bins=50, range=(0, 5), density=True)[0]
        # Compute the CDF
        CY = np.cumsum(Y) / Y.sum()
        # ax.axvline(x=X[(CY <= 0.999).sum()], label="P99.9", color="red")
        # ax.axvline(x=X[(CY <= 0.99).sum()], label="P99", color="orange")
        # ax.axvline(
        #     x=X[(CY <= 0.9).sum()],
        #     label=f"{checkpoint} p90",
        #     color=COLORS[checkpoint],
        #     linestyle=":",
        # )
        # ax.axvline(
        #     x=X[(CY <= 0.5).sum()],
        #     label=f"{checkpoint} p50",
        #     color=COLORS[checkpoint],
        #     linestyle="--",
        # )
        # ax.axvline(
        #     x=online_data.mean(),
        #     label=f"Avg-{checkpoint}",
        #     color=COLORS[checkpoint],
        #     linestyle="-.",
        # )
        ax.plot(
            X,
            CY,
            label=LABELS[checkpoint],
            color=COLORS[checkpoint],
            linestyle=LINE_STYLES[checkpoint],
            linewidth=1.4,
        )
    # ax.set_title("Latency CDF of online update")
    ax.set_xlabel("Latency (s)", labelpad=5)
    ax.set_ylabel("Cumulative Distribution", labelpad=5)
    major_ticks = np.linspace(0, 5, 6)
    minor_ticks = np.linspace(0, 5, 21)
    ax.set_xticks(major_ticks, major=True)
    ax.set_xticks(minor_ticks, minor=True)
    major_ticks = np.linspace(0, 1, 6)
    minor_ticks = np.linspace(0, 1, 21)
    ax.set_yticks(major_ticks, major=True)
    ax.set_yticks(minor_ticks, minor=True)

    ax.grid(which="major", axis="x")
    ax.grid(which="minor", axis="x", alpha=0.2)
    ax.grid(which="major", axis="y")
    ax.grid(which="minor", axis="y", alpha=0.2)
    ax.set_ylim(bottom=0, top=1)
    ax.set_xlim(left=0, right=5)
    lgd = ax.legend(
        loc="lower right",
        ncol=1,
        labelspacing=0.4,
        columnspacing=0.4,
        handlelength=1.5,
        handletextpad=0.8,
    )
    lgd.get_frame().set_linewidth(0.0)

    fig.tight_layout()
    fig.savefig("plots/exp3-online-cdf.png")
    fig.savefig("plots/exp3-online-cdf.pdf")


def _get_batch_range():
    return np.arange(
        np.ceil(WARMUP_EPOCHS / BATCH_UPDATE_INTERVAL) * BATCH_UPDATE_INTERVAL,
        N_EPOCHS,
        BATCH_UPDATE_INTERVAL,
    )


def plot_checkpoint_batch(checkpoint_options: List[str]):
    fig, ax = plt.subplots()
    for checkpoint in checkpoint_options:
        data = read_workflow_with_checkpoint(checkpoint)
        batch_data = data["batch_update_durations"]
        ax.plot(
            _get_batch_range(),
            batch_data,
            label=checkpoint,
            color=COLORS[checkpoint],
        )
    ax.set_title("Latency of batch update")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Latency (s)")
    fig.legend()
    fig.tight_layout()
    fig.savefig("plots/exp3-batch.png")
    fig.savefig("plots/exp3-batch.pdf")


def plot_difference():
    with open("result/difference.txt") as f:
        diffs = [float(n) for n in f.readlines()]
    fig, ax = plt.subplots()
    ax.plot(_get_batch_range(), diffs)
    ax.set_title("Differences in score between batch update & online update")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Delta in score")
    fig.savefig("plots/exp3-difference.png")
    fig.savefig("plots/exp3-difference.pdf")


if __name__ == "__main__":
    plt.rc("font", size=24, family="Times")
    plt.rc("xtick", labelsize=22)
    plt.rc("ytick", labelsize=22)
    checkpoint_options = ["async", "True", "False"]
    plot_checkpoint_online(checkpoint_options)
    plot_checkpoint_batch(checkpoint_options)
    plot_difference()
