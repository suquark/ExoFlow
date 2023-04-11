import argparse
import json
import time

import ray
from ray import workflow
from ray.workflow.debug_utils import run_workflow_local, resume_workflow_local

import common
import utils
import pipeline


def run_with_config(config, debug: bool = False):
    workflow_id = config.pop(
        "workflow_id",
        f"exp3-{config['checkpoint']}-{utils.get_datetime().replace(':', '-')}",
    )
    config.pop("failure", None)
    resume = config.pop("resume", False)

    ray.init("local", num_cpus=32, storage=common.STORAGE)
    dag = pipeline.generate_pipeline(
        n_epochs=common.N_EPOCHS,
        n_warmup_epochs=common.WARMUP_EPOCHS,
        checkpoint_mode=config["checkpoint"],
    )

    if not resume:
        start = time.time()
        with open("start_time", "w") as f:
            f.write(str(start))
        if debug:
            run_workflow_local(dag, workflow_id=workflow_id)
        else:
            workflow.run(dag, workflow_id=workflow_id)
    else:
        with open("start_time") as f:
            start = float(f.read())
        if debug:
            resume_workflow_local(workflow_id=workflow_id)
        else:
            workflow.resume(workflow_id=workflow_id)

    return time.time() - start


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="graph example")
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
        "--disable-ephemeral-tasks",
        help="use pure workflow tasks instead when specified",
        action="store_true",
    )
    parser.add_argument("--workflow-id", help="Workflow ID", type=str, default="")
    parser.add_argument(
        "--failure", help="the failure case we are testing", type=str, default=""
    )
    parser.add_argument("--resume", help="resume the workflow", action="store_true")

    args = parser.parse_args()

    config = dict(
        checkpoint=args.checkpoint,
        autoscale=False,
        use_ephemeral_tasks=not args.disable_ephemeral_tasks,
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

    duration = run_with_config(config.copy(), debug=common.DEBUG)
    print(f"duration={duration}")

    with open(log_file, "a") as f:
        f.write(
            f"duration = {round(duration, 3)}s | config = {json.dumps(config)} | "
            f"datetime = {utils.get_datetime()} | "
            f"version = {utils.get_git_revision_short_hash()}\n"
        )
