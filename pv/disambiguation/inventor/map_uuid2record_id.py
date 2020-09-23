from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string('input', 'exp_out/inventor/run_24/disambiguation.tsv', '')
flags.DEFINE_string('uuidmap', 'data/inventor/uuid2mentionid.tsv', '')
flags.DEFINE_string('output', 'exp_out/inventor/run_24/disambiguation.tsv.toScore', '')
logging.set_verbosity(logging.INFO)


def main(argv):
    logging.info('loading map of inventor mentions...')
    records = dict()
    with open(FLAGS.uuidmap, 'r') as fin:
        for line in tqdm(fin, desc='load', total=17000000):
            splt = line.strip().split("\t")
            records[splt[0]] = splt[1]
    logging.info('loading map of inventor mentions...')
    with open(FLAGS.output, 'w') as fout:
        with open(FLAGS.input, 'r') as fin:
            for line in tqdm(fin, desc='process', total=17000000):
                splt = line.strip().split('\t')
                if splt[0] in records:
                    splt[0] = records[splt[0]]
                    fout.write('%s\n' % '\t'.join(splt))


if __name__ == "__main__":
    app.run(main)
