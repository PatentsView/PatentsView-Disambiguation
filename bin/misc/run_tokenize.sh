#!/usr/bin/env bash

set -exu

infile=${1:-'patent.tsv'}
output_dir=${2:-'patent_sents'}
chunk=${3:-0}
chunksize=${4:-10000}

python  -m pv.disambiguation.util.sent_tokenize  \
--infile $infile \
--output_dir $output_dir \
--chunk_id $chunk \
--chunk_size $chunksize