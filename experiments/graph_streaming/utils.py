import argparse
import contextlib
import datetime
import shutil
import os
import sys
import threading
import time

import pytz
import subprocess


def argconv(**convs):
    def parse_argument(arg):
        if arg in convs:
            return convs[arg]
        else:
            msg = "invalid choice: {!r} (choose from {})"
            choices = ", ".join(sorted(repr(choice) for choice in convs.keys()))
            raise argparse.ArgumentTypeError(msg.format(arg, choices))

    return parse_argument


def split_into_chunks(arr, n_chunks: int):
    """Divide an array into chunks evenly."""
    size = len(arr) // n_chunks
    ext = len(arr) % n_chunks
    for i in range(n_chunks):
        start = i * size + min(i, ext)
        yield arr[start : start + size + int(i < ext)]


def get_git_revision_hash() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("ascii").strip()


def get_git_revision_short_hash() -> str:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except subprocess.CalledProcessError:
        return "<unknown>"


def get_datetime() -> str:
    dt = datetime.datetime.now(pytz.timezone("US/Pacific"))
    return dt.isoformat()


def _prefix_path(path: str, prefix: str) -> str:
    d = os.path.dirname(path)
    n = os.path.basename(path)
    return os.path.join(d, prefix + n)


def _check_exists_and_normalize(full_path: str, prefix: bool = False) -> bool:
    if prefix:
        swap_path = _prefix_path(full_path, "swap-")
    else:
        swap_path = full_path + ".swap"
    commit_path = full_path + ".commit"
    swap_exists = os.path.exists(swap_path)
    full_path_exists = os.path.exists(full_path)
    commit_path_exists = os.path.exists(commit_path)

    if not commit_path_exists:
        if swap_exists:
            shutil.rmtree(swap_path, ignore_errors=True)
        if full_path_exists:
            shutil.rmtree(full_path, ignore_errors=True)
        return False

    if swap_exists and full_path_exists:
        shutil.rmtree(swap_path, ignore_errors=True)
        return True
    if swap_exists and not full_path_exists:
        os.rename(swap_path, full_path)
        return True
    if not swap_exists and full_path_exists:
        return True
    return False


@contextlib.contextmanager
def open_atomic(path: str, mode="r"):
    """Open file with atomic file writing support. File reading is also
    adapted to atomic file writing (for example, the backup file
    is used when an atomic write failed previously.)

    TODO(suquark): race condition like two processes writing the
    same file is still not safe. This may not be an issue, because
    in our current implementation, we only need to guarantee the
    file is either fully written or not existing.

    Args:
        path: The file path.
        mode: Open mode same as "open()".

    Returns:
        File object.
    """
    if "a" in mode or "+" in mode or "x" in mode:
        raise ValueError("Atomic open does not support appending or creating new.")
    swap_path = path + ".swap"
    commit_path = path + ".commit"

    exists = _check_exists_and_normalize(path)
    if "r" in mode:  # read mode
        if exists:
            f = open(path, mode)
        else:
            raise FileNotFoundError(path)
        try:
            yield f
        finally:
            f.close()
    elif "w" in mode:  # overwrite mode
        f = open(swap_path, mode)
        try:
            yield f
            with open(commit_path, "wb"):
                pass
            shutil.rmtree(path, ignore_errors=True)
            os.replace(swap_path, path)
        finally:
            f.close()
    else:
        raise ValueError(f"Unknown file open mode {mode}.")


def read_atomic(path: str, handler):
    _check_exists_and_normalize(path, prefix=True)
    return handler(path)


def write_atomic(path: str, obj, handler):
    _check_exists_and_normalize(path, prefix=True)
    swap_path = _prefix_path(path, "swap-")
    commit_path = path + ".commit"
    handler(swap_path, obj)
    with open(commit_path, "wb"):
        pass
    shutil.rmtree(path, ignore_errors=True)
    os.replace(swap_path, path)


_threads = []


def detect_and_fail(name=None, delay=0):
    global _threads

    def _crash_task(name: str, timeout=0):
        print("!" * 20 + f"crashing task [{name}]" + "!" * 20, file=sys.stderr)
        time.sleep(timeout)
        os.kill(os.getpid(), 9)

    def _crash_cluster(name: str, timeout=0):
        print("!" * 20 + f"crashing cluster [{name}]" + "!" * 20, file=sys.stderr)
        time.sleep(timeout)
        os.system("ray stop --force")

    if not name:
        from exoflow.workflow_context import get_current_task_id

        name = get_current_task_id()

    # for distributed case, working dir could be different
    name = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
    if os.path.exists(name + ".task_crash"):
        os.unlink(name + ".task_crash")
        if delay > 0:
            t = threading.Timer(delay, _crash_task, [name])
            t.start()
            _threads.append(t)
        else:
            _crash_task(name)
    elif os.path.exists(name + ".cluster_crash"):
        os.unlink(name + ".cluster_crash")
        if delay > 0:
            t = threading.Timer(delay, _crash_cluster, [name])
            t.start()
            _threads.append(t)
        else:
            _crash_cluster(name)


def crash_task(name: str):
    with open(f"{name}.task_crash", "w"):
        pass


def crash_cluster(name: str):
    with open(f"{name}.cluster_crash", "w"):
        pass


def gpu_exists() -> bool:
    try:
        subprocess.check_output("nvidia-smi")
        print("Nvidia GPU detected!")
        return True
    except Exception:
        # This command not being found can raise quite a few different
        # errors depending on the configuration.
        print("No Nvidia GPU in system!")
        return False
