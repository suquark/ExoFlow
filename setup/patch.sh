#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
patch_dir=$(realpath $SCRIPT_DIR/../patch)

ray_path=$(dirname $(python -c 'import ray; print(ray.__file__)'))

cp $patch_dir/storage.py $ray_path/_private/storage.py

rm -r $ray_path/dag
cp -r $patch_dir/dag $ray_path/dag
