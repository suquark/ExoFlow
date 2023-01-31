#!/bin/bash

# -------------------- Setup R --------------------

# https://github.com/Rdatatable/data.table/wiki/Amazon-EC2-for-beginners

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
sudo add-apt-repository 'deb  http://cran.stat.ucla.edu/bin/linux/ubuntu trusty/'
sudo apt-get update
sudo apt-get -y install r-base-core
sudo apt-get -y install libcurl4-openssl-dev    # for RCurl which devtools depends on
sudo apt-get -y install htop                    # to monitor RAM and CPU

# https://github.com/h2oai/db-benchmark
cd ~/efs
git clone https://github.com/h2oai/db-benchmark.git
cd db-benchmark


# -------------------- Setup Spark --------------------

sudo apt-get install openjdk-8-jdk
pip install --upgrade pyspark
# check installation
python -c "import pyspark; print(pyspark.__version__)"


# -------------------- Generate data table --------------------

# R
# install.packages("data.table")
rm -r data
Rscript _data/groupby-datagen.R 1e7 1e2 0 0
mkdir -p data
mv *.csv data/

# -------------------- Run experiment --------------------

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SRC_DATANAME=$(ls data/ | grep csv | sed 's/.csv//g')
python spark/groupby-spark.py
