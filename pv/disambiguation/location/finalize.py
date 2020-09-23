

from tqdm import tqdm

import os
from absl import flags
from absl import app
from absl import logging

import pickle

FLAGS = flags.FLAGS

flags.DEFINE_string('input', 'exp_out/location/run_8', '')
flags.DEFINE_string('output', 'exp_out/location/run_8/disambiguation.tsv', '')

logging.set_verbosity(logging.INFO)


def process_file(fout, pkl_file):
    res = pickle.load(open(pkl_file, 'rb'))
    for c, res in res.items():
        for idx in range(len(res[0])):
            fout.write('%s\t%s\n' % (res[0][idx], res[1][idx]))

def process(fout, rundir):
    for f in tqdm([f for f in os.listdir(rundir) if f.endswith('.pkl')]):
        process_file(fout, os.path.join(rundir, f))

def main(argv):
    with open(FLAGS.output, 'w') as fout:
        process(fout, FLAGS.input)

if __name__ == "__main__":
    app.run(main)