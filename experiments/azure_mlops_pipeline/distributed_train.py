# https://keras.io/guides/distributed_training/
import json
import os
import time
from pathlib import Path

import ray
from utils import open_atomic, read_atomic, write_atomic, detect_and_fail
import global_config
import torch
import torch.distributed as dist
from torch import nn
from torch._utils import _flatten_dense_tensors, _unflatten_dense_tensors
from torch.utils.data.dataset import TensorDataset

import numpy as np
from torchvision import models
import tqdm


def info(msg, char="#", width=75):
    print("")
    print(char * width)
    print(char + "   %0*s" % ((-1 * width) + 5, msg) + char)
    print(char * width)


@ray.remote
class Trainer:
    def __init__(
        self,
        world_size: int,
        rank: int,
        img_size=160,
        learning_rate=0.0001,
        model_path="model",
        seed=42,
        use_gpu: bool = False,
    ):
        dist.init_process_group(
            backend="nccl" if use_gpu else "gloo",
            init_method="tcp://127.0.0.1:45678",
            world_size=world_size,
            rank=rank,
        )
        dist.all_reduce(torch.ones(1).cuda() if use_gpu else torch.ones(1))
        self.data_parallel_group = torch.distributed.new_group(list(range(world_size)))

        self.model = None
        self.all_parameters = None
        self.optimizer = None

        self.model_ckpt = Path(model_path).joinpath("latest.h5")
        self.metadata_path = Path(model_path).joinpath("metadata.json")
        self.img_size = img_size
        self.learning_rate = learning_rate
        self.seed = seed
        self._use_gpu = use_gpu

        self._rank = rank
        self._world_size = world_size
        self._iteration = 0
        self._last_training_duration = None
        self._last_checkpointing_duration = None
        self._last_data_fetch_duration = None
        self.restore()

    def allreduce_params(self, reduce_after=True, no_scale=False, fp32_allreduce=False):
        # adopted from
        # https://github.com/NVIDIA/Megatron-LM/blob/main/megatron/model/distributed.py
        buckets = {}
        for param in self.all_parameters:
            if param.requires_grad and param.grad is not None:
                tp = param.data.type()
                if tp not in buckets:
                    buckets[tp] = []
                buckets[tp].append(param)
        for tp in buckets:
            bucket = buckets[tp]
            grads = [param.grad.data for param in bucket]
            coalesced = _flatten_dense_tensors(grads)
            if fp32_allreduce:
                coalesced = coalesced.float()
            if not no_scale and not reduce_after:
                coalesced /= dist.get_world_size(group=self.data_parallel_group)
            dist.all_reduce(coalesced, group=self.data_parallel_group)
            if self._use_gpu:
                torch.cuda.synchronize()
            if not no_scale and reduce_after:
                coalesced /= dist.get_world_size(group=self.data_parallel_group)
            for buf, synced in zip(grads, _unflatten_dense_tensors(coalesced, grads)):
                buf.copy_(synced)

    def _patch_model(self):
        self.model.classifier[2] = nn.Linear(self.model.classifier[2].in_features, 1)

    def _restore_or_create(self):
        # model
        torch.manual_seed(self.seed)
        if self.model_ckpt.exists():
            info("Restoring Model")
            self.model = models.convnext_tiny(weights=None)
            state_dict = read_atomic(str(self.model_ckpt), handler=torch.load)
            self._patch_model()
            self.model.load_state_dict(state_dict["model"])
        else:
            info("Creating Model")
            self.model = models.convnext_tiny(models.ConvNeXt_Tiny_Weights)
            self._patch_model()

        # convert the model to GPU before binding the optimizer
        if self._use_gpu:
            self.model = self.model.to("cuda")

        self.all_parameters = list(self.model.parameters())
        self.optimizer = torch.optim.Adam(self.all_parameters, lr=3e-4)

        if self.model_ckpt.exists():
            self.optimizer.load_state_dict(state_dict["optimizer"])

    def _fit(self, dataset_chunks, epochs: int, batch_size: int):
        start_time = time.time()
        dataset_chunks = ray.get(dataset_chunks)

        ds = None
        for x, y in dataset_chunks:
            # get subset of batch. this is a temporary approach
            x = x[self._rank :: self._world_size]
            y = y[self._rank :: self._world_size]
            x = x.transpose((0, 3, 1, 2)).astype(np.float32)
            x = torch.from_numpy(x)
            y = torch.from_numpy(y).reshape(x.size(0), 1)
            _ds = TensorDataset(x, y)
            ds = _ds if ds is None else ds + _ds

        dataloader = torch.utils.data.dataloader.DataLoader(ds, batch_size=batch_size)

        # training
        if self._rank == 0:
            info(f"[Iteration {self._iteration}] Training")

        loss_fn = torch.nn.BCEWithLogitsLoss()

        if self._rank == 0:
            data_iter = tqdm.tqdm(dataloader, total=len(dataloader))
        else:
            data_iter = dataloader
        for xs, ys in data_iter:
            self.optimizer.zero_grad()
            if self._use_gpu:
                xs, ys = xs.cuda(), ys.cuda()
            pred = self.model(xs)
            loss = loss_fn(pred, ys)
            loss.backward()
            self.allreduce_params()
            self.optimizer.step()

        if self._rank == 0:
            print("Loss: ", loss.item())
        self._last_training_duration = time.time() - start_time
        self._iteration += epochs

    def fit(self, dataset_chunks, batch_size: int, target_epoch: int):
        if self._rank == 0:
            detect_and_fail(f"train_actor_{self._iteration}")
        epochs = target_epoch - self._iteration
        if epochs > 0:
            self._fit(dataset_chunks, epochs, batch_size)

    def restore(self):
        if self.metadata_path.exists():
            with open_atomic(str(self.metadata_path)) as f:
                self._iteration = json.load(f)["iteration"]

        self._restore_or_create()

    def checkpoint(self):
        if self._rank != 0:
            return
        start_time = time.time()
        # save model
        info("Saving Model")

        print("Serializing h5 model to:\n{}".format(self.model_ckpt))

        def _handler(path: str, _):
            state_dict = {
                "model": self.model.state_dict(),
                "optimizer": self.optimizer.state_dict(),
                "iteration": self._iteration,
            }
            torch.save(state_dict, path)

        write_atomic(str(self.model_ckpt), None, _handler)
        with open_atomic(str(self.metadata_path), "w") as f:
            json.dump({"iteration": self._iteration}, f)
        self._last_checkpointing_duration = time.time() - start_time

    def summary(self):
        return {
            "training_time": self._last_training_duration,
            "checkpointing_time": self._last_checkpointing_duration,
        }


@ray.remote
def create_trainer(
    img_size=160, learning_rate=0.0001, model_path="model", use_gpu=False
):
    os.system("sudo kill -9 $(sudo lsof -t -i:45678)")

    try:
        trainers = []
        for i in range(global_config.N_TRAINERS):
            trainers.append(ray.get_actor(name=f"trainer_{i}"))
        info("Get the existing actor successfully")
    except Exception:
        for i in range(global_config.N_TRAINERS):
            try:
                t = ray.get_actor(name=f"trainer_{i}")
                ray.kill(t)
                time.sleep(1)
            except Exception:
                pass
        trainers = []
        for i in range(global_config.N_TRAINERS):
            if use_gpu:
                info("Creating model with GPUs")
                trainer_cls_with_options = Trainer.options(
                    num_gpus=1, name=f"trainer_{i}", **global_config.TRAIN_OPTIONS
                )
            else:
                info("Creating model with CPUs")
                option = global_config.TRAIN_OPTIONS.copy()
                option["num_cpus"] = 4
                trainer_cls_with_options = Trainer.options(
                    name=f"trainer_{i}", **option
                )
            trainer = trainer_cls_with_options.remote(
                world_size=global_config.N_TRAINERS,
                rank=i,
                img_size=img_size,
                learning_rate=learning_rate,
                model_path=model_path,
                use_gpu=use_gpu,
            )
            trainers.append(trainer)
    # detect_and_fail(delay=30)
    return trainers


@ray.remote
def fit(trainers, dataset, epochs=1, batch_size=32):
    from exoflow.workflow_context import get_current_task_id

    detect_and_fail()
    task_id = get_current_task_id()
    target_epoch = int(task_id.replace("train_", "")) + epochs
    outputs = []
    for trainer in trainers:
        outputs.append(trainer.fit.remote(dataset, batch_size, target_epoch))
    ray.get(outputs)
    return trainers


@ray.remote
def finalize(trainers):
    outputs = []
    for trainer in trainers:
        outputs.append(trainer.checkpoint.remote())
    ray.get(outputs)
