

from absl import app
import collections
import numpy as np

from sklearn.feature_extraction.text import TfidfVectorizer

import os
from absl import flags
from absl import app
from absl import logging
import numpy as np
import pickle
import wandb
import time
from pathos.multiprocessing import ProcessingPool
from pathos.threading import ThreadPool

FLAGS = flags.FLAGS
flags.DEFINE_string('eval_cc',
                    '/iesl/canvas/nmonath/research/entity-resolution/er/data/inventor-train/eval_common_characteristics.', '')
flags.DEFINE_string('eval_mixture', '/iesl/canvas/nmonath/research/entity-resolution/er/data/inventor-train/eval_mixture.', '')
flags.DEFINE_string('canopy2record_dict', 'data/inventor/canopy2record_first_letter_last_name_dict.pkl', '')
flags.DEFINE_string('record2canopy_dict', 'data/inventor/record2canopy_first_letter_last_name_dict.pkl', '')
flags.DEFINE_integer('mod', 4, '')

logging.set_verbosity(logging.INFO)

def load_all_evaluation_ids(eval_files):
    ids = collections.defaultdict(set)
    for fn in eval_files:
        with open(fn) as fin:
            for line in fin:
                splt = line.strip().split('\t')
                ids[splt[1]].add(splt[0])
    return ids

def split_train_dev(id_map, mod, record2canopy):
    to_remove = set([x for cluster in id_map.values() for x in cluster if x not in record2canopy])
    logging.info('records that seem to be missing %s', str(to_remove))
    canopies = list(set([record2canopy[x] for cluster in id_map.values() for x in cluster if x in record2canopy]))
    canopy_sizes = collections.defaultdict(int)
    canopy2entities = collections.defaultdict(set)
    for cid, cluster in id_map.items():
        for x in cluster:
            if x in record2canopy:
                canopy2entities[record2canopy[x]].add(cid)
                canopy_sizes[record2canopy[x]] += 1

    sorted_by_size = sorted(canopies, key=lambda x: canopy_sizes[x], reverse=True)
    train_e = dict()
    dev_e = dict()
    dev_canopies = dict()
    train_canopies = dict()
    for idx, canopy in enumerate(sorted_by_size):
        valid_for_train = all([e not in dev_e for e in canopy2entities[canopy]])
        valid_for_dev = all([e not in train_e for e in canopy2entities[canopy]])
        if (idx+1) % mod == 0 and valid_for_dev:
            dev_canopies[canopy] = canopy_sizes[canopy]
            logging.debug('tried for dev, valid_for_dev, dev %s %s', canopy, canopy_sizes[canopy])
            for e in canopy2entities[canopy]:
                dev_e[e] = id_map[e].difference(to_remove)
        elif valid_for_train:
            train_canopies[canopy] = canopy_sizes[canopy]
            logging.debug('tried for train, train %s %s', canopy, canopy_sizes[canopy])
            for e in canopy2entities[canopy]:
                train_e[e] = id_map[e].difference(to_remove)
        else:
            assert valid_for_dev
            dev_canopies[canopy] = canopy_sizes[canopy]
            logging.info('tried for train, valid_for_dev, dev %s %s', canopy, canopy_sizes[canopy])
            for e in canopy2entities[canopy]:
                dev_e[e] = id_map[e].difference(to_remove)

    logging.info('len(train) = %s', len(train_e))
    logging.info('len(dev) = %s', len(dev_e))

    return train_e, dev_e, train_canopies, dev_canopies

def write_file(id_map, filename):
    with open(filename, 'w') as fout:
        for k,v in id_map.items():
            for vv in v:
                fout.write('%s\t%s\n' % (vv,k))

def write_canopy_file(id_map, filename):
    with open(filename, 'w') as fout:
        for k,v in id_map.items():
            fout.write('%s\t%s\n' % (k,v))

def main(argv):

    with open(FLAGS.record2canopy_dict, 'rb') as fin:
        record2canopy = pickle.load(fin)

    eval_cc_ids = load_all_evaluation_ids([FLAGS.eval_cc + 'train_dev'])
    train_cc_e, dev_cc_e, train_canopies, dev_canopies = split_train_dev(eval_cc_ids, FLAGS.mod, record2canopy)
    write_file(train_cc_e, FLAGS.eval_cc + 'train')
    write_file(dev_cc_e, FLAGS.eval_cc + 'dev')
    write_canopy_file(train_canopies, FLAGS.eval_cc + 'train.canopies')
    write_canopy_file(dev_canopies, FLAGS.eval_cc + 'dev.canopies')

    eval_mixture_ids = load_all_evaluation_ids([FLAGS.eval_mixture + 'train_dev'])
    train_mixture_e, dev_mixture_e, train_canopies, dev_canopies = split_train_dev(eval_mixture_ids, FLAGS.mod, record2canopy)
    write_file(train_mixture_e, FLAGS.eval_mixture + 'train')
    write_file(dev_mixture_e, FLAGS.eval_mixture + 'dev')
    write_canopy_file(train_canopies, FLAGS.eval_mixture + 'train.canopies')
    write_canopy_file(dev_canopies, FLAGS.eval_mixture + 'dev.canopies')

if __name__ == "__main__":
    app.run(main)