#!/usr/bin/env bash

set -exu

TEXT_DATA=$1
MODEL=$2
threads=$3

fasttext sent2vec -input $TEXT_DATA -output $MODEL -dim 700 -neg 10 -thread $threads -ws 30 -wordNgrams 2 -dropoutK 4
