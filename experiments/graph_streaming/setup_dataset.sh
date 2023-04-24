#!/bin/bash

# twitter dataset:
# An edge from i to j indicates that j is a follower of i
# https://snap.stanford.edu/data/twitter-2010.html

mkdir -p /exoflow/twitter_dataset
cd /exoflow/twitter_dataset
echo "Downloading twitter dataset..."
wget https://snap.stanford.edu/data/twitter-2010.txt.gz
wget https://snap.stanford.edu/data/twitter-2010-ids.csv.gz
echo "Decompressing twitter dataset..."
gzip -d twitter-2010.txt.gz
echo "Splitting twitter dataset..."
python /exoflow/experiments/graph_streaming/split_dataset.py
# gzip -d twitter-2010-ids.csv.gz
