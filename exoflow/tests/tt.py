import ray
from ray import workflow

@ray.remote
def source1():
    return "[source1]"


@ray.remote
def append1(x):
    return x + "[append1]"


@ray.remote
def append2(x):
    return x + "[append2]"


@ray.remote
def simple_sequential():
    x = source1.bind()
    y = append1.bind(x)
    return workflow.continuation(append2.bind(y))


if __name__ == '__main__':
    # This test also shows different "style" of running workflows.
    assert (
        workflow.create(simple_sequential.bind()).run() == "[source1][append1][append2]"
    )