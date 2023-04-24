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

#### Setup (20-40 minutes)

First, deploy the serverless functions by running the following command:

```bash
/exoflow/experiments/stateful_serverless/deploy.sh
```

Second, setup the gateway for Beldi.

1. Go to the AWS console, click the Lambda. ![Lambda Console](images/lambda_service.png)
2. In the Lambda console, click `function` ![Lambda Function](images/lambda_function.png)
3. Search for `beldi-dev-gateway` and click it. ![Beldi Gateway](images/beldi_gateway.png)
4. On the page of the function, click `add trigger` and select `API Gateway`. ![Add Trigger](images/add_trigger.png) ![API Gateway](images/api_gateway.png)
5. On the page of the trigger, config like below. Then add the trigger. ![HTTP API](images/http_api.png)
6. This is what you should see after adding the trigger. ![Trigger Added](images/trigger_added.png). You can check the URL of the gateway by clicking the trigger on the page.
7. Run `/exoflow/experiments/stateful_serverless/get_beldi_gateway.sh`. You will see it returns the URL of the gateway same as above, if everything is setup correctly.


#### Figure 7(a)


#### Figure 7(b)

### 5.3 Online-offline graph processing

#### Figure 7(c)

## Microbenchmark

### Figure 8(a)

### Figure 8(b)

### Figure 8(c)
