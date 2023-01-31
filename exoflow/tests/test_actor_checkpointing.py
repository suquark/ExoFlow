from ray.tests.conftest import *  # noqa

import pytest
import ray
from ray import workflow


def test_workflow_with_actor(workflow_start_regular_shared):
    @ray.remote
    class Actor:
        def __init__(self):
            self.n = 0
            self.checkpoint_count = 0

        def inc(self):
            self.n += 1

        def get(self):
            return self.n, self.checkpoint_count

        def checkpoint(self):
            self.checkpoint_count += 1

    @ray.remote
    def create_actor():
        return Actor.remote()

    @ray.remote
    def inc(actor):
        actor.inc.remote()
        return actor

    @ray.remote
    def get(actor):
        return ray.get(actor.get.remote())

    actor = create_actor.bind()
    for _ in range(10):
        actor = inc.bind(actor)
    dag = get.bind(actor)
    count, checkpoint_count = workflow.run(dag)
    assert count == 10
    assert checkpoint_count == 11


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
