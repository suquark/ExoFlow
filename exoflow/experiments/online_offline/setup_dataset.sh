#!/bin/bash

# twitter dataset:
# https://snap.stanford.edu/data/twitter-2010.html

mkdir -p ~/efs/twitter_dataset
cd ~/efs/twitter_dataset
wget https://snap.stanford.edu/data/twitter-2010.txt.gz
wget https://snap.stanford.edu/data/twitter-2010-ids.csv.gz
#gzip -d twitter-2010.txt.gz
#gzip -d twitter-2010-ids.csv.gz
# An edge from i to j indicates that j is a follower of i

