#!/usr/bin/env bash

set -exu

chunk_id=$1
threads=$2

export MKL_NUM_THREADS=$threads
export OPENBLAS_NUM_THREADS=$threads
export OMP_NUM_THREADS=$threads


python pv/disambiguation/location/run_clustering.py $chunk_id