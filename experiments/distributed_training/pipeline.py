import exoflow
from exoflow.common import CheckpointModeType

import etl
import transform
import distributed_train as train
import autoscaler
import global_config


def generate_etl_pipeline(
    data_source: str,
    checkpoint: CheckpointModeType,
    dataset_dir: str,
    use_ephemeral_tasks: bool,
    n_partitions: int,
    n_transform_tasks: int,
    dataset_fraction: float,
    enhance_dataset_multiplier: int,
):
    """Generate pipeline.

    #partitions = #shuffle_tasks / #transform_tasks, this is locked.

    There are four dependency sets:
    load >> data[0..n]
    data[n] >> data[n+1]
    train[n] >> train[n+1]
    data[n] >> train[n] >> data[n+2]
    """
    if checkpoint == "hybrid":
        etl_checkpoint = "async"
    else:
        etl_checkpoint = checkpoint

    extract_task = etl.download.options(
        **exoflow.options(checkpoint=etl_checkpoint),
        **global_config.ETL_OPTIONS,
    ).bind(
        data_source, dataset_dir, force_clear=global_config.FORCE_CLEAR_DOWNLOAD_DATA
    ) | etl.extract.options(
        **exoflow.options(checkpoint=etl_checkpoint),
        **global_config.ETL_OPTIONS,
    )

    train_files = etl.get_train_files.options(
        **exoflow.options(checkpoint=etl_checkpoint),
        **global_config.ETL_OPTIONS,
    ).bind(dataset_dir, fraction=dataset_fraction)
    extract_task >> train_files

    stage_dir = etl.stage_files.options(
        **exoflow.options(checkpoint=etl_checkpoint),
        **global_config.ETL_OPTIONS,
    ).bind(
        dataset_dir,
        "stage_1",
        train_files,
        enhance_dataset_multiplier=enhance_dataset_multiplier,
    )

    filtered_stage_dir = etl.filter_dataset.options(
        **exoflow.options(checkpoint=etl_checkpoint),
        **global_config.ETL_OPTIONS,
    ).bind(dataset_dir, "stage_2", prev_stage_path=stage_dir)

    dataset_chunks = etl.preprocess_dataset.options(
        **exoflow.options(
            isolation=True, checkpoint=etl_checkpoint, name="preprocess"
        ),
        **global_config.ETL_OPTIONS,
    ).bind(
        filtered_stage_dir,
        n_partitions=n_partitions,
        n_transform_tasks=n_transform_tasks,
        use_ephemeral_tasks=use_ephemeral_tasks,
    )
    waiter = etl.wait_and_cleanup.options(**global_config.ETL_OPTIONS).bind(
        dataset_chunks,
        dataset_dir=dataset_dir,
        force_clear=global_config.FORCE_CLEAR_DOWNLOAD_DATA,
    )
    return dataset_chunks, waiter


def generate_train_pipeline(
    dataset_chunks,
    etl_waiter,
    checkpoint: CheckpointModeType,
    model_checkpoint_path: str,
    use_gpu: bool,
    autoscale: bool,
    use_ephemeral_tasks: bool,
    n_epochs: int,
    batch_size: int,
    image_size: int,
    n_transform_tasks: int,
    n_transform_tasks_limit: int,
):
    """Generate pipeline.

    #partitions = #shuffle_tasks / #transform_tasks, this is locked.

    There are four dependency sets:
    load >> data[0..n]
    data[n] >> data[n+1]
    train[n] >> train[n+1]
    data[n] >> train[n] >> data[n+2]
    """
    if checkpoint == "hybrid":
        transform_checkpoint = False
    else:
        transform_checkpoint = checkpoint

    trainer = train.create_trainer.options(
        **exoflow.options(isolation=True), **global_config.TRAIN_OPTIONS
    ).bind(model_path=model_checkpoint_path, use_gpu=use_gpu)

    transform_tasks = []
    train_tasks = []
    for i in range(n_epochs):
        # TODO(suquark): how would Ray handle pending resources?
        #  would Ray schedule tasks so that it unblocks the
        #  lineage as soon as possible?
        n_transform_tasks = autoscaler.autoscale_transform.options(
            **global_config.TRAIN_OPTIONS
        ).bind(
            i,
            n_transform_tasks,
            n_transform_tasks_limit,
            enable_autoscale=autoscale,
        )
        if i == 0:
            etl_waiter >> n_transform_tasks
        if i >= 1:
            transform_tasks[i - 1] >> n_transform_tasks
        if i >= 2:
            train_tasks[i - 2] >> n_transform_tasks

        shuffled_train_dataset = transform.transform_dataset.options(
            **exoflow.options(name=f"transform_{i}", checkpoint=transform_checkpoint),
            **global_config.TRAIN_OPTIONS,
        ).bind(
            dataset_chunks,
            image_size,
            n_epoch=i,
            n_transform_tasks=n_transform_tasks,
            use_ephemeral_tasks=use_ephemeral_tasks,
        )
        trainer = train.fit.options(
            **exoflow.options(name=f"train_{i}", checkpoint=(i % 5 == 0)),
            **global_config.TRAIN_OPTIONS,
        ).bind(
            trainer,
            shuffled_train_dataset,
            batch_size=batch_size,
        )

        transform_tasks.append(shuffled_train_dataset)
        train_tasks.append(trainer)

    # val_files = etl.get_val_files.bind(dataset_dir)
    # val_dataset = etl.preprocess_dataset.options(**checkpoint_option).bind(val_files)
    # return combine.bind(trainer, val_dataset)

    return train.finalize.bind(trainer)


def generate_pipeline(
    data_source: str,
    checkpoint: CheckpointModeType,
    model_checkpoint_path: str,
    dataset_dir: str,
    use_gpu: bool,
    autoscale: bool,
    use_ephemeral_tasks: bool,
    n_epochs: int,
    batch_size: int,
    image_size: int,
    n_partitions: int,
    n_transform_tasks: int,
    n_transform_tasks_limit: int,
    dataset_fraction: float,
    enhance_dataset_multiplier: int,
):
    """Generate pipeline.

    #partitions = #shuffle_tasks / #transform_tasks, this is locked.

    There are four dependency sets:
    load >> data[0..n]
    data[n] >> data[n+1]
    train[n] >> train[n+1]
    data[n] >> train[n] >> data[n+2]
    """
    dataset_chunks, etl_waiter = generate_etl_pipeline(
        data_source=data_source,
        checkpoint=checkpoint,
        dataset_dir=dataset_dir,
        use_ephemeral_tasks=use_ephemeral_tasks,
        n_partitions=n_partitions,
        n_transform_tasks=n_transform_tasks,
        dataset_fraction=dataset_fraction,
        enhance_dataset_multiplier=enhance_dataset_multiplier,
    )
    dag = generate_train_pipeline(
        dataset_chunks,
        etl_waiter=etl_waiter,
        checkpoint=checkpoint,
        model_checkpoint_path=model_checkpoint_path,
        use_gpu=use_gpu,
        autoscale=autoscale,
        use_ephemeral_tasks=use_ephemeral_tasks,
        n_epochs=n_epochs,
        batch_size=batch_size,
        image_size=image_size,
        n_transform_tasks=n_transform_tasks,
        n_transform_tasks_limit=n_transform_tasks_limit,
    )
    return dag
