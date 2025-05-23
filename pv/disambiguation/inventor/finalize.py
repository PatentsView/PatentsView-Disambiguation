import os
import pickle

from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string('input', 'exp_out/inventor/run_32', '')
flags.DEFINE_string('output', 'exp_out/inventor/run_32/disambiguation.tsv', '')

logging.set_verbosity(logging.INFO)


def process_file(fout, pkl_file):
    res = pickle.load(open(pkl_file, 'rb'))
    for c, res in res.items():
        for idx in range(len(res[0])):
            fout.write('%s\t%s\n' % (res[0][idx], res[1][idx]))


def process(fout, rundir):
    for f in tqdm([f for f in os.listdir(rundir) if
                   f.endswith('.pkl') and 'internals' not in f and 'job' in f and 'update' not in f]):
        process_file(fout, os.path.join(rundir, f))


def finalize(config):
    output_file = "{}/disambiguation.tsv".format(config['inventor']['clustering_output_folder'])
    clustering_output_directory = os.path.join(config['inventor']['clustering_output_folder'])
    with open(output_file, "w") as fout:
        process(fout, clustering_output_directory)


def main(argv):
    with open(FLAGS.output, 'w') as fout:
        process(fout, FLAGS.input)


if __name__ == "__main__":
    app.run(main)
