import ray
import exoflow

import common
from ingest import create_ingest_actors
from partition import run_epoch
from online_update import create_online_actors, online_update
from batch_update import compute_tunk_rank


@ray.remote
def combine(x, y):
    return None


def generate_pipeline(n_epochs: int, n_warmup_epochs: int, checkpoint_mode):
    state = common.get_initial_state(20)
    online_update_results = []
    batch_update_results = []

    extras = {}  # dict(resources={"tag:online": 0.001})

    actors = create_ingest_actors.options(
        **exoflow.options(checkpoint=False), **extras
    ).bind(common.N_INGESTION, common.N_EVENT_INTERVAL)
    online_updater = create_online_actors.options(
        **exoflow.options(checkpoint=False), **extras
    ).bind()
    online_output, batch_output = None, None
    for i in range(1, n_epochs + 1):
        if checkpoint_mode == "hybrid":
            if i % common.N_CHECKPOINT_INTERVAL == 0:
                workflow_options = exoflow.options(checkpoint="async")
            else:
                workflow_options = exoflow.options(checkpoint=False)
        else:
            workflow_options = exoflow.options(checkpoint=checkpoint_mode)

        state = run_epoch.options(**workflow_options, **extras).bind(actors, state, i)
        online_output = online_update.options(**workflow_options, **extras).bind(
            online_updater, state, i
        )
        if len(online_update_results) > 0:
            online_update_results[-1] >> online_output
        online_update_results.append(online_output)
        if i % common.BATCH_UPDATE_INTERVAL == 0 and i >= n_warmup_epochs:
            batch_output = compute_tunk_rank.options(
                **exoflow.options(checkpoint=True), **extras
            ).bind(state, i)
            if len(batch_update_results) > 0:
                batch_update_results[-1] >> batch_output
            batch_update_results.append(batch_output)

    return combine.bind(online_output, batch_output)
