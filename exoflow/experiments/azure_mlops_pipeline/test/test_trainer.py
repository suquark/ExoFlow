import ray
import torch
import torch.distributed as dist


@ray.remote(num_gpus=1)
class Trainer:
    def __init__(
        self,
        world_size: int,
        rank: int,
    ):
        dist.init_process_group(
            backend="nccl",
            init_method="tcp://127.0.0.1:45678",
            world_size=world_size,
            rank=rank,
        )
        dist.all_reduce(torch.zeros(1).cuda())

    def poll(self):
        pass


if __name__ == "__main__":
    xs = []
    ks = []
    for i in range(4):
        r = Trainer.remote(4, i)
        xs.append(r)
        ks.append(r.poll.remote())

    ray.get(ks)
