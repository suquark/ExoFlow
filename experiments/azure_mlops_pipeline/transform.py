import random

import numpy as np
import ray
import exoflow
import tensorflow as tf

from utils import split_into_chunks, detect_and_fail
import global_config


# https://www.tensorflow.org/tutorials/images/data_augmentation
@ray.remote(**global_config.TRAIN_OPTIONS)
def augment_dataset_chunk(dataset_chunks, image_size, n_epoch: int, chunk_index: int):
    chunk_seed = n_epoch * 10 ** 6 + chunk_index * 10 ** 3
    if chunk_index == 0:
        detect_and_fail(f"transform_subtask_{n_epoch}")

    IMAGE_SIZE = [image_size, image_size, 3]
    random.seed(chunk_seed)

    dataset_chunks = ray.get(dataset_chunks)
    n_images = 0
    for xs, _ in dataset_chunks:
        n_images += xs.shape[0]

    new_xs = np.empty((n_images, *IMAGE_SIZE), dtype=np.float)
    new_ys = np.concatenate([ys for _, ys in dataset_chunks])
    i = 0
    for xs, _ in dataset_chunks:
        for x in xs:
            if random.random() < 0.25:
                x = tf.image.resize(
                    x, [int(image_size * 1.225), int(image_size * 1.225)]
                )
                x = tf.image.stateless_random_crop(
                    x, [image_size, image_size, 3], seed=(i, chunk_seed)
                )
            else:
                x = tf.image.resize(x, [image_size, image_size])

            x = tf.image.stateless_random_flip_left_right(x, seed=(i, chunk_seed + 1))

            if random.random() < 0.25:
                x = tf.image.stateless_random_brightness(
                    x, max_delta=0.95, seed=(i, chunk_seed + 2)
                )
            # x = tf.image.stateless_random_contrast(
            #     x, lower=0.1, upper=0.9, seed=seed,
            # )
            # x = tf.image.stateless_random_flip_up_down(x, seed)
            # x = tf.image.stateless_random_hue(x, 0.2, seed)
            if random.random() < 0.25:
                x = tf.image.stateless_random_saturation(
                    x, 0.5, 1.0, seed=(i, chunk_seed + 3)
                )
            if random.random() < 0.25:
                x = tf.image.stateless_random_jpeg_quality(
                    x, 75, 95, seed=(i, chunk_seed + 4)
                )
            new_xs[i] = x
            i += 1

    rng = np.random.default_rng(seed=chunk_seed)
    rng.shuffle(new_xs)
    rng = np.random.default_rng(seed=chunk_seed)
    rng.shuffle(new_ys)
    return new_xs, new_ys


@ray.remote
def transform_dataset(
    dataset_chunks,
    image_size,
    n_epoch: int,
    n_transform_tasks: int,
    use_ephemeral_tasks: bool,
):
    print(
        f"[augment_and_shuffle_dataset: "
        f"Epoch {n_epoch}] n_tasks: {n_transform_tasks}, "
        f"augmenting data..."
    )
    rng = np.random.default_rng(seed=n_epoch)
    rng.shuffle(dataset_chunks)

    if use_ephemeral_tasks:
        func = augment_dataset_chunk.remote
    else:
        func = augment_dataset_chunk.bind

    chunk_splits = split_into_chunks(dataset_chunks, n_chunks=n_transform_tasks)
    datasets = [
        func(chunk, image_size, n_epoch, chunk_index=i)
        for i, chunk in enumerate(chunk_splits)
    ]
    detect_and_fail()
    return datasets if use_ephemeral_tasks else workflow.continuation(datasets)
