import argparse
import json
import os
import shutil
import time

import ray
import exoflow
from exoflow.debug_utils import run_workflow_local, resume_workflow_local

import pipeline
import utils
import global_config

# https://www.tensorflow.org/install/pip


def run_with_config(config, debug: bool = False):
    wf_dir = os.path.abspath("workflow_dir")
    dataset_dir = os.path.abspath("dataset_dir")
    workflow_id = config.pop("workflow_id", utils.get_datetime().replace(":", "-"))
    config.pop("failure", None)
    resume = config.pop("resume", False)

    dag = pipeline.generate_pipeline(
        # original: https://aiadvocate.blob.core.windows.net/public/tacodata.zip
        data_source="https://siyuan-public.s3.us-west-2.amazonaws.com/tacodata.zip",
        model_checkpoint_path=wf_dir,
        use_gpu=utils.gpu_exists(),
        dataset_dir=dataset_dir,
        image_size=160,
        n_epochs=global_config.N_EPOCHS,
        batch_size=global_config.BATCH_SIZE * config["enhance_dataset_multiplier"],
        n_partitions=4,
        n_transform_tasks=config["enhance_dataset_multiplier"],
        n_transform_tasks_limit=16,
        dataset_fraction=global_config.DATASET_FRACTION,
        **config,
    )

    def ray_init():
        if not resume:
            shutil.rmtree(wf_dir, ignore_errors=True)
            os.makedirs(wf_dir, exist_ok=True)
        if global_config.FORCE_CLEAR_DOWNLOAD_DATA:
            shutil.rmtree(dataset_dir, ignore_errors=True)
        else:
            shutil.rmtree(dataset_dir + "/train", ignore_errors=True)
            shutil.rmtree(dataset_dir + "/val", ignore_errors=True)
        if global_config.local_test:
            ray.init("local", num_cpus=16, storage=wf_dir)
        else:
            ray.init("auto")

    ray_init()

    if not resume:
        start = time.time()
        with open("start_time", "w") as f:
            f.write(str(start))
        if debug:
            run_workflow_local(dag, workflow_id=workflow_id)
        else:
            exoflow.run(dag, workflow_id=workflow_id)
            # Ray only: ray.get(dag.execute())
    else:
        with open("start_time") as f:
            start = float(f.read())
        if debug:
            resume_workflow_local(workflow_id=workflow_id)
        else:
            exoflow.resume(workflow_id=workflow_id)

    return time.time() - start


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="train example")
    parser.add_argument(
        "--checkpoint",
        help="checkpoint_mode",
        type=utils.argconv(
            **{
                "false": False,
                "true": True,
                "async": "async",
                "hybrid": "hybrid",
            }
        ),
        default="false",
    )
    parser.add_argument(
        "--enhance-dataset-multiplier",
        help="enhance-dataset-multiplier",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--disable-ephemeral-tasks",
        help="use pure workflow tasks instead when specified",
        action="store_true",
    )
    parser.add_argument("--workflow-id", help="Workflow ID", type=str, default="")
    parser.add_argument(
        "--failure", help="the failure case we are testing", type=str, default=""
    )
    parser.add_argument("--resume", help="resume the workflow", action="store_true")
    parser.add_argument(
        "--skip-record", help="skip writing records to log file", action="store_true"
    )

    args = parser.parse_args()

    config = dict(
        checkpoint=args.checkpoint,
        autoscale=False,
        use_ephemeral_tasks=not args.disable_ephemeral_tasks,
        enhance_dataset_multiplier=args.enhance_dataset_multiplier,
    )
    if args.workflow_id:
        config["workflow_id"] = args.workflow_id
    log_file = "results.txt"
    if args.failure:
        config["failure"] = args.failure
        log_file = "fault_tolerance_results.txt"
        if not args.resume:
            with open(args.failure, "w"):
                pass
    if args.resume:
        config["resume"] = args.resume
        log_file = "fault_tolerance_results.txt"

    duration = run_with_config(config.copy(), debug=global_config.DEBUG)
    print(f"duration={duration}")

    if not args.skip_record:
        with open(log_file, "a") as f:
            f.write(
                f"duration = {round(duration, 3)}s | config = {json.dumps(config)} | "
                f"datetime = {utils.get_datetime()} | "
                f"version = {utils.get_git_revision_short_hash()}\n"
            )
