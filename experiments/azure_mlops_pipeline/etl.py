# https://github.com/kubeflow/examples/blob/9665ff8a5c47d8f75dd0010a77f0a0d8298a7756/pipelines/azurepipeline/code/pipeline.py
# https://github.com/microsoft/MLOps/tree/master/examples/KubeflowPipeline

from typing import List, Tuple

import os
import shutil
import zipfile
from pathlib import Path

import numpy as np
import ray
import tensorflow as tf
import wget
import exoflow, ObjectRef

from utils import split_into_chunks, detect_and_fail
import global_config


def check_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return Path(path).resolve(strict=False)


@ray.remote
def download(source, target, force_clear=False) -> Path:
    if force_clear and os.path.exists(target):
        print("Removing {}...".format(target))
        shutil.rmtree(target)

    check_dir(target)

    targt_file = Path(target).joinpath("tacodata.zip")
    if targt_file.exists() and not force_clear:
        print("data already exists, skipping download")
        return targt_file

    if source.startswith("http"):
        print("Downloading from {} to {}".format(source, target))
        wget.download(source, str(targt_file))
        print("Done!")
    else:
        print("Copying from {} to {}".format(source, target))
        shutil.copyfile(source, targt_file)
    return targt_file


def load_dataset(path: Path) -> List[Path]:
    print("Scanning {}".format(path))
    # find subdirectories in base path
    # (they should be the catagories)
    catagories = []
    for d in path.iterdir():
        if d.is_dir():
            catagories.append(d.name)
    print(f"found {catagories}")

    imgs = []
    for d in catagories:
        tmp_path = path / d
        print("Processing {}".format(tmp_path))
        # only care about files in directory
        for f in tmp_path.iterdir():
            if f.is_file() and not f.name.lower().endswith(".jpg"):
                print(f"skipping {f}")
                continue
            imgs.append(f)
    return imgs


@ray.remote
def extract(target_file: Path) -> Path:
    print("Unzipping {}".format(target_file))
    zipr = zipfile.ZipFile(target_file)
    zipr.extractall(target_file.parent)
    zipr.close()

    return target_file.parent


@ray.remote
def get_train_files(dataset_dir: str, fraction: float = 1) -> List[Path]:
    train_files = load_dataset(Path(dataset_dir) / "train")
    return train_files[: int(len(train_files) * fraction)]


@ray.remote
def get_val_files(dataset_dir: str, fraction: float = 1) -> List[Path]:
    val_files = load_dataset(Path(dataset_dir) / "val")
    return val_files[: int(len(val_files) * fraction)]


@ray.remote
def stage_files(
    dataset_dir: str,
    stage_name: str,
    img_files: List[Path],
    enhance_dataset_multiplier: int,
) -> Path:
    dest_paths = []
    stage_dir = Path(dataset_dir) / stage_name
    stage_dir.mkdir(exist_ok=True)
    # TODO: this is a temporary solution to increase the dataset size
    for i in range(enhance_dataset_multiplier):
        for f in img_files:
            dest = stage_dir / f"{f.parent.name}-{i}-{f.name}"
            dest_paths.append(dest)
            shutil.copyfile(f, dest)
    return stage_dir


def load_image(path):
    img_raw = tf.io.read_file(path)
    img_tensor = tf.image.decode_jpeg(img_raw, channels=3)
    # img_final = tf.image.resize(img_tensor, [image_size, image_size]) / 255
    # return img_final
    return img_tensor


@ray.remote
def filter_dataset(dataset_dir: str, stage_name: str, prev_stage_path: Path) -> Path:
    stage_dir = Path(dataset_dir) / stage_name
    stage_dir.mkdir(exist_ok=True)
    for f in prev_stage_path.iterdir():
        try:
            img = load_image(str(f))
        except Exception:
            continue
        if img.shape[2] != 3:
            continue
        if 0.1 < img.shape[0] / img.shape[1] < 10:
            shutil.copyfile(f, stage_dir / f.name)
    return stage_dir


@ray.remote(**global_config.ETL_OPTIONS)
def preprocess_dataset_chunk(
    img_files: List[Path], height: int, width: int, chunk_id: int
) -> Tuple:
    RAW_IMAGE_SIZE = [height, width, 3]
    n_images = len(img_files)
    rng = np.random.default_rng(seed=chunk_id)
    rng.shuffle(img_files)
    xs = np.empty((n_images, *RAW_IMAGE_SIZE), dtype=np.uint8)
    ys = np.empty(n_images, dtype=np.float)

    for i, f in enumerate(img_files):
        img = load_image(str(f))
        img = tf.image.resize(img, RAW_IMAGE_SIZE[:2])
        if f.name.startswith("burrito"):
            label = 0
        elif f.name.startswith("tacos"):
            label = 1
        else:
            assert False
        xs[i], ys[i] = img, label

    return xs, ys


@ray.remote
def preprocess_dataset(
    prev_stage_path: Path,
    n_partitions: int,
    n_transform_tasks: int,
    use_ephemeral_tasks: bool,
) -> List:
    n_load_tasks = n_partitions * n_transform_tasks
    print(f"[preprocess_dataset] n_tasks: {n_load_tasks}")
    img_files = list(prev_stage_path.iterdir())

    # get the average image size
    shapes = []
    for f in img_files:
        img = load_image(str(f))
        shapes.append(img.shape[:2])
    avg_shape = np.mean(shapes, axis=0)
    avg_img_height, avg_img_width = round(avg_shape[0]), round(avg_shape[1])

    print(
        f"Dataset size: {len(img_files)}, "
        f"average height: {avg_img_height}, "
        f"average width: {avg_img_width}"
    )

    rng = np.random.default_rng(seed=21)
    rng.shuffle(img_files)
    assert len(img_files) >= n_load_tasks

    if use_ephemeral_tasks:
        chunk_proc = preprocess_dataset_chunk.remote
    else:
        chunk_proc = preprocess_dataset_chunk.bind

    chunk_splits = split_into_chunks(img_files, n_chunks=n_load_tasks)
    chunks = [
        chunk_proc(chunk, avg_img_height, avg_img_width, chunk_id=i)
        for i, chunk in enumerate(chunk_splits)
    ]

    detect_and_fail(delay=global_config.PREPROCESS_FAIL_DELAY)
    return chunks if use_ephemeral_tasks else workflow.continuation(chunks)


@ray.remote
def wait_and_cleanup(
    dataset_chunks: List[ObjectRef], dataset_dir: str, force_clear=False
) -> None:
    ray.get(dataset_chunks)
    if force_clear and os.path.exists(dataset_dir):
        print("Removing {}...".format(dataset_dir))
        shutil.rmtree(dataset_dir)
