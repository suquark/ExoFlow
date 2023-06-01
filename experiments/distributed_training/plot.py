from collections import namedtuple, defaultdict
import json

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import cm

SETTINGS = [
    "Selective AsyncCkpt",
    "NoCkpt",
    "AsyncCkpt",
    "SyncCkpt",
    "Workflow Tasks",
]

COLORS = {s: cm.tab10(i) for i, s in enumerate(SETTINGS)}


def parse_line(line: str):
    segs = line.split("|")
    duration = float(segs[0].replace("duration = ", "").strip()[:-1])
    config = json.loads(segs[1].replace("config = ", "").strip())
    config.pop("workflow_id", None)
    return config, duration


# key = (config["enhance_dataset_multiplier"], config["checkpoint"])

BenchmarkRecord = namedtuple("ExpRecord", ("config", "duration"))


def parse_results(path, line_parser):
    records = []
    with open(path) as f:
        for line in f:
            config, duration = line_parser(line)
            key = list(config.items())
            key.sort(key=lambda x: x[0])
            records.append((tuple(key), duration))
    assert records
    groups = defaultdict(list)
    for k, v in records:
        groups[k].append(v)
    new_records = [
        BenchmarkRecord(k, (np.mean(v), np.std(v))) for k, v in groups.items()
    ]
    new_records.sort(key=lambda x: tuple(str(v) for k, v in x[0]))
    return new_records


def filter_records(records, **config):
    for k, v in config.items():
        records = [r for r in records if (k, v) in r.config]
    return records


def project_records(records, vectorize=True):
    durations = [r.duration for r in records]
    if vectorize:
        return tuple(zip(*durations))


def get_data(records, **config):
    output = project_records(filter_records(records, **config))
    if not output:
        print(f"Warning: no data for '{config}', returning empty data instead")
        return (0,), (0,)
    return output


def get_normal_data(n_repeat: int):
    records = parse_results(f"result/results.txt", line_parser=parse_line)

    def _pick_and_repeat(d):
        return [d[-1]] * n_repeat

    data = [
        ("Selective AsyncCkpt", get_data(records, checkpoint="hybrid")),
        ("NoCkpt", get_data(records, checkpoint=False, use_ephemeral_tasks=True)),
        ("SyncCkpt", get_data(records, checkpoint=True)),
    ]

    bar_data = [
        {
            "height": _pick_and_repeat(x[1][0]),
            "yerr": _pick_and_repeat(x[1][1]),
            # "label": x[0],
        }
        for x in data
    ]
    return bar_data


def get_fault_tolerance_data():
    records = parse_results(
        f"result/fault_tolerance_results.txt", line_parser=parse_line
    )

    failures = [
        "train_12.cluster_crash",
        "preprocess.task_crash",
        "train_actor_8.task_crash",
        "transform_8.task_crash",
        "transform_subtask_8.task_crash",
    ]

    def _get_data(checkpoint):
        mean, std = [], []
        for f in failures:
            _mean, _std = get_data(records, checkpoint=checkpoint, failure=f)
            assert len(_mean) == len(_std) == 1
            _mean, _std = _mean[0], _std[0]
            mean.append(_mean)
            std.append(_std)
        return mean, std

    data = [
        ("Selective AsyncCkpt", _get_data(checkpoint="hybrid")),
        ("NoCkpt", _get_data(checkpoint=False)),
        ("SyncCkpt", _get_data(checkpoint=True)),
    ]

    bar_data = [
        {"height": x[1][0], "yerr": x[1][1], "label": x[0], "color": COLORS[x[0]]}
        for x in data
    ]
    return bar_data


def plot_duration(ax: plt.Axes):
    records = parse_results(f"result/results.txt", line_parser=parse_line)

    labels = [f"{i}x" for i in range(1, 5)]

    x = np.arange(len(labels))  # the label locations

    data = [
        ("Selective AsyncCkpt", get_data(records, checkpoint="hybrid")),
        ("NoCkpt", get_data(records, checkpoint=False, use_ephemeral_tasks=True)),
        ("AsyncCkpt", get_data(records, checkpoint="async")),
        ("SyncCkpt", get_data(records, checkpoint=True)),
        ("Workflow Tasks", get_data(records, use_ephemeral_tasks=False)),
    ]

    bar_data = [
        {"height": x[1][0], "yerr": x[1][1], "label": x[0], "color": COLORS[x[0]]}
        for x in data
    ]

    width = 0.14  # the width of the bars
    _ = ax.bar(x - 2 * width, width=width, **bar_data[0])
    _ = ax.bar(x - 1 * width, width=width, **bar_data[1])
    _ = ax.bar(x + 0 * width, width=width, **bar_data[2])
    _ = ax.bar(x + 1 * width, width=width, **bar_data[3])
    _ = ax.bar(x + 2 * width, width=width, **bar_data[4])

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel("Duration (s)", labelpad=5)
    ax.set_xlabel("Dataset Size", labelpad=10)
    ax.set_xticks(x, labels)

    major_ticks = np.linspace(0, 1200, 7)
    ax.set_yticks(major_ticks, major=True)
    ax.set_ylim(bottom=0, top=1100)

    # ax.set_title("Training workflow (25 epochs)")
    # ax.bar_label(rects1, padding=3)
    # ax.bar_label(rects2, padding=3)
    # ax.bar_label(rects3, padding=3)
    # ax.legend(loc="upper center", ncol=5)
    # fig.tight_layout()
    # fig.savefig("plots/exp1-duration.png")
    # fig.savefig("plots/exp1-duration.pdf")


def plot_fault_tolerance(ax: plt.Axes):
    labels = [
        "Cluster",  # workflow_task_object_lost
        "Ingest data",  # cluster crash
        "Train actor",  # ephemeral actor crash
        "Aug. task",  # workflow task crash
        "Aug. data",  # ephemeral task crash
    ]

    bar_data_1 = get_normal_data(len(labels))
    bar_data_2 = get_fault_tolerance_data()

    x = np.arange(len(labels))  # the label locations
    width = 0.18  # the width of the bars

    _ = ax.bar(x - 1 * width, width=width, **bar_data_2[0])
    _ = ax.bar(x + 0 * width, width=width, **bar_data_2[1])
    _ = ax.bar(x + 1 * width, width=width, **bar_data_2[2])

    _ = ax.bar(x - 1 * width, width=width, **bar_data_1[0], color="black", alpha=0.4)
    _ = ax.bar(x + 0 * width, width=width, **bar_data_1[1], color="black", alpha=0.4)
    _ = ax.bar(x + 1 * width, width=width, **bar_data_1[2], color="black", alpha=0.4)

    major_ticks = np.linspace(0, 1500, 7)
    ax.set_yticks(major_ticks, major=True)
    ax.set_ylim(bottom=0, top=1350)

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel("Duration (s)", labelpad=5)
    ax.set_xlabel("Failure Type", labelpad=10)
    # ax.set_title("Training workflow fault tolerance")
    ax.set_xticks(x, labels)
    # fig.tight_layout()
    # fig.savefig("plots/exp1-fault-tolerance.png")
    # fig.savefig("plots/exp1-fault-tolerance.pdf")


if __name__ == "__main__":
    # Use Type 1 fonts
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    plt.rc("font", size=27.5, family="Times New Roman")
    fig, (ax1, ax2) = plt.subplots(
        ncols=2, figsize=(20, 5.5), gridspec_kw={"width_ratios": [4, 5]}
    )

    plot_duration(ax1)
    plot_fault_tolerance(ax2)

    # share the legend
    handles, labels = ax1.get_legend_handles_labels()
    lgd = fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=5,
        bbox_to_anchor=(0.52, 1),
        labelspacing=1,
        columnspacing=1.5,
        handletextpad=0.5,
        borderpad=0,
    )
    lgd.get_frame().set_linewidth(0.0)
    ax1.grid(which="both", axis="y", ls=":")
    ax2.grid(which="both", axis="y", ls=":")
    fig.tight_layout()
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width, box.height * 0.9])
    box = ax2.get_position()
    ax2.set_position([box.x0, box.y0, box.width, box.height * 0.9])
    fig.savefig("plots/exp1.png")
    fig.savefig("plots/exp1.pdf")
