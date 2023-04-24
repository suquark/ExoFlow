# ExoFlow: A universal workflow system for exactly-once DAGs

**OSDI'23 Artifact Evaluation**

## Overview

### Local Setup

The local setup is required to launch the clusters for the experiments.

We encourage using a [Conda](https://docs.conda.io/en/latest/miniconda.html) environment for the local setup for isolation. The following commands will create a Conda environment named `exoflow` and install all the dependencies.

```bash
conda create -n exoflow python=3.8.13
conda activate exoflow
# We install `grpcio` separately to handle Apple M1 chip issues.
conda install 'grpcio<=1.43.0' -y
pip install awscli boto3 ray==2.0.1
```

https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html

### Setup Remote Shared Cluster

After your local setup is ready, you can launch the shared cluster by running the following command:

```bash
ray up -y <Your Local ExoFlow Github Repository>/clusters/shared.yaml
```

After the cluster is ready, follow the instructions on your screen to login the cluster.

Then you need to setup your AWS credentials on the cluster. This enables automation of the experiments. 

On the cluster, first create the `.aws` directory: 

```bash
mkdir -p ~/.aws
```

Then edit the `~/.aws/credentials` file (e.g., `vim ~/.aws/credentials`) and add the following content:

```
[default]
region=us-east-1
aws_access_key_id=<Your AWS Access Key ID>
aws_secret_access_key=<Your AWS Secret Access Key>
```

If you do not know your AWS credentials, you can follow the instructions [here](https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html) to create or get them. Here is an image that shows how to create the access key in for your AWS credential:

![AWS Access Key](images/create_access_key.png)

Finally, change the permission of the `~/.aws/credentials` file to `600` to secure your credentials:

```bash
chmod 600 ~/.aws/credentials
```

## Main Results

### 5.1 ML training pipelines

To run the experiment, first start the cluster by running the following command:

```bash
cd <Your Local ExoFlow Github Repository>/clusters
ray up -y distributed_training_cluster.yaml
```

After the cluster is fully ready, initialize the cluster by running the following command:

```bash
cd <Your Local ExoFlow Github Repository>/clusters
./init_s3_access.sh
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

#### Setup Serverless Functions (20-40 minutes)

First, deploy the serverless functions by running the following command:

```bash
/exoflow/experiments/stateful_serverless/deploy.sh
# deploy shared functions for ExoFlow
/exoflow/experiments/stateful_serverless/deploy-exoflow.sh
```

Second, setup the gateway for Beldi.

1. Go to the AWS console, click the Lambda. ![Lambda Console](images/lambda_service.png)
2. In the Lambda console, click `function` ![Lambda Function](images/lambda_function.png)
3. Search for `beldi-dev-gateway` and click it. ![Beldi Gateway](images/beldi_gateway.png)
4. On the page of the function, click `add trigger` and select `API Gateway`. ![Add Trigger](images/add_trigger.png) ![API Gateway](images/api_gateway.png)
5. On the page of the trigger, config like below. Then add the trigger. ![HTTP API](images/http_api.png)
6. This is what you should see after adding the trigger. ![Trigger Added](images/trigger_added.png). You can check the URL of the gateway by clicking the trigger on the page.
7. Run `/exoflow/experiments/stateful_serverless/get_beldi_gateway.sh`. You will see it returns the URL of the gateway same as above, if everything is setup correctly.

#### Setup ExoFlow Server (20 minutes)

TODO: add instructions

#### Figure 7(a)

**Beldi**

(~75 min) Batch running of all experiments with the following command:

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/batch-beldi.sh"
```

(*recommanded*) you can run the experiments one by one with the rate (i.e. throughput) you want (7-10 min):

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-beldi.sh $rate"
```

Check Beldi results in `/exoflow/experiments/stateful_serverless/result/beldi/`

**ExoFlow**

(~75 min) Batch running of all experiments with the following command:

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/batch-exoflow.sh"
```

(*recommanded*) you can run the experiments one by one with the rate (i.e. throughput) you want (7-10 min):

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-exoflow.sh $rate"
```

Check ExoFlow results by running `python /exoflow/experiments/stateful_serverless/collect_metrics.py` and check the `workflow-server` field in `/exoflow/experiments/stateful_serverless/result/result.json`. The array in field represents the latency under the throughtput of `[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]` requests per second.

**ExoFlow-Failure**

This experiment need this extra deployment:

```bash
/exoflow/experiments/stateful_serverless/deploy-exoflow-ft.sh
```

NOTE: this deployment over writes the previous ExoFlow deployment. If you want to run the previous experiments, you need to redeploy the serverless functions (`deploy-exoflow.sh`).

(~75 min) Batch running of all experiments with the following command:

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/batch-exoflow-failure.sh"
```

(*recommanded*) you can run the experiments one by one with the rate (i.e. throughput) you want (7-10 min):

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-exoflow-failure.sh $rate"
```

Check ExoFlow-Failure results by running `python /exoflow/experiments/stateful_serverless/collect_metrics.py` and check the `workflow-server-failure` field in `/exoflow/experiments/stateful_serverless/result/result.json`. The array in field represents the latency under the throughtput of `[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]` requests per second.

#### Figure 7(b)

**Beldi**

(7-10 min) Beldi baseline:

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-beldi-reserve.sh"
```

(7-10 min) "-WAL"

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-exoflow-reserve.sh reserve_serial"
```

(7-10 min) "+parallel"

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-exoflow-reserve.sh reserve"
```

(7-10 min) "+async"

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-exoflow-reserve.sh reserve_overlapckpt"
```

(7-10 min) "-async"

```bash
docker exec -w /root/beldi -it beldi bash -ic "/stateful_serverless/benchmark/benchmark-exoflow-reserve.sh reserve_nooverlapckpt"
```

Check results by running `python /exoflow/experiments/stateful_serverless/collect_metrics.py` and look into `/exoflow/experiments/stateful_serverless/result/result.json`. Here is the mapping between the field and the figure:

* beldi-cloudwatch-reserve -> Beldi
* reserve_serial -> +WAL
* reserve -> +parallel
* reserve_overlapckpt -> +async
* reserve_nooverlapckpt -> -async

You could further run `python /exoflow/experiments/stateful_serverless/plot.py` to plot Figure 7(a) and 7(b). The generated figures are saved in `/exoflow/experiments/stateful_serverless/plots`.

### 5.3 Online-offline graph processing

#### Figure 7(c)

## Microbenchmark

### Figure 8(a)

### Figure 8(b)

### Figure 8(c)
