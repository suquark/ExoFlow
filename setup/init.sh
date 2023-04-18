#!/bin/bash

# Ray setup will fail initially, but we can still ssh into it to perform setup.

# ssh-keygen
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
cat <<'EOT' >> ~/.ssh/config
Host *
    StrictHostKeyChecking no
EOT

# https://aws.amazon.com/releasenotes/aws-deep-learning-ami-gpu-pytorch-1-13-ubuntu-20-04/
# init image: ami-02b509fb28354de85
conda init bash
source ~/.bashrc
# conda create -n exoflow-dev python=3.8 -y
conda create --name exoflow-dev --clone pytorch

# https://unix.stackexchange.com/questions/705447/what-is-the-difference-between-cat-eof-and-cat-eot-and-when-should-i-use-it
cat <<'EOT' >> ~/.bashrc
conda activate exoflow-dev
export PATH="/home/ubuntu/.bazel/bin:$PATH"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREF
EOT

sudo apt-get update
sudo apt-get -y install git binutils ncdu
git clone https://github.com/aws/efs-utils
pushd efs-utils
./build-deb.sh
sudo apt-get -y install ./build/amazon-efs-utils*deb
popd
rm -rf efs-utils

# pip install ~/efs/ray-3.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl
pip install ray==2.0.1
pip install -r requirements.txt
# old version:
# torch==1.12.1
# torchvision==0.13.1
conda install pytorch torchvision torchaudio pytorch-cuda=11.6 -c pytorch -c nvidia -y

sudo ln -s ~/efs/spark-3.3.0-bin-hadoop3/ /spark

# patch Ray
./patch.sh
