#!/usr/bin/env bash

set -exu

sweep_id=$1
threads=$2

export MKL_NUM_THREADS=$threads
export OPENBLAS_NUM_THREADS=$threads
export OMP_NUM_THREADS=$threads

wandb agent $sweep_id