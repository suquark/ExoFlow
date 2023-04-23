# ExoFlow: A universal workflow system for exactly-once DAGs

**OSDI'23 Artifact Evaluation**

## Overview


```bash
conda create -n exoflow python=3.8.13
conda activate exoflow
pip install awscli boto3 ray==2.0.1
```

## Main Results

### 5.1 ML training pipelines



#### Figure 6 (left)

To run the experiment, first start the cluster by running the following command:

```bash
cd <Your Local ExoFlow Gtihub Repository>/clusters
ray up -y distributed_training_cluster.yaml
```

#### Figure 6 (left)

```bash
cd /exoflow/experiments/distributed_training
./run.sh
```

#### Figure 6 (right)

```bash
cd /exoflow/experiments/distributed_training
./run.sh
```

### 5.2 Stateful serverless workflows

#### Figure 7(a)


#### Figure 7(b)

### 5.3 Online-offline graph processing

#### Figure 7(c)

## Microbenchmark

### Figure 8(a)

### Figure 8(b)

### Figure 8(c)
