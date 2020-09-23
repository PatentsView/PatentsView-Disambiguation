import os
import pickle

import torch
import torch.nn
import wandb
from absl import app
from absl import flags
from absl import logging
from grinch.agglom import Agglom
from tqdm import tqdm

from pv.disambiguation.core import load_inventor_mentions
from pv.disambiguation.inventor.run_clustering import FLAGS
from pv.disambiguation.inventor.model import InventorModel
from grinch.eval_pw_f1 import eval_micro_pw_f1

flags.DEFINE_string('model_path',
                    '',
                    'tsv of the ids')

flags.DEFINE_string('training_data',
                    'data/evaluation-data/inventor/handlabeled/gold-labels.txt',
                    'tsv of the ids')

flags.DEFINE_string('dev_data',
                    'data/evaluation-data/inventor/handlabeled/gold-labels.txt',
                    'tsv of the ids')

flags.DEFINE_integer('min_dev_size', 10,
                     'tsv of the ids')

flags.DEFINE_integer('max_dev_size', 20000,
                     'tsv of the ids')

flags.DEFINE_string('cache_dir', 'cache/', '')
flags.DEFINE_integer('num_samples', 50000, '')
flags.DEFINE_integer('num_negatives', 5, '')
flags.DEFINE_integer('batch_size', 100, '')
flags.DEFINE_integer('dev_every', 1000, '')
flags.DEFINE_integer('epochs', 3, '')
flags.DEFINE_float('margin', 0.1, '')
flags.DEFINE_float('lr', 0.0001, '')

flags.DEFINE_integer('num_thresholds', 500, '')
flags.DEFINE_integer('max_dev_canopy_size', 5000, '')
flags.DEFINE_integer('max_num_dev_canopies', 50, '')

flags.DEFINE_string('canopy2record_dict', 'data/inventor/canopy2record_first_letter_last_name_dict.pkl', '')
flags.DEFINE_string('record2canopy_dict', 'data/inventor/record2canopy_first_letter_last_name_dict.pkl', '')


def main(argv):
    logging.info('Running clustering with argv %s', str(argv))
    wandb.init(project="%s-%s" % (FLAGS.exp_name, FLAGS.dataset_name))
    wandb.config.update(flags.FLAGS)

    outdir = os.path.join(FLAGS.outprefix, wandb.env.get_project(), os.environ.get(wandb.env.SWEEP_ID, 'solo'),
                          wandb.env.get_run())

    logging.info('outdir %s', outdir)

    os.makedirs(outdir, exist_ok=True)

    os.makedirs(FLAGS.cache_dir, exist_ok=True)

    need_to_load_train = False
    dev_cache_file = os.path.join(FLAGS.cache_dir, 'dev-%s.pkl' % os.path.basename(FLAGS.dev_data))

    need_to_load_dev = False
    if os.path.exists(dev_cache_file):
        logging.info('Using cached dev data! %s', dev_cache_file)
        with open(dev_cache_file, 'rb') as fin:
            dev_collection = pickle.load(fin)
            dev_datasets = dev_collection[0]
            dev_set_tmp = dict()
            for canopy in dev_datasets.keys():
                pids, lbls, records = dev_datasets[canopy]
                if len(records) >= FLAGS.min_dev_size and len(records) <= FLAGS.max_dev_size:
                    dev_set_tmp[canopy] = [pids, lbls, records]
            dev_datasets = dev_set_tmp
    else:
        need_to_load_dev = True

    if need_to_load_train or need_to_load_dev:
        logging.info('need_to_load_train %s', need_to_load_train)
        logging.info('need_to_load_dev %s', need_to_load_dev)
        logging.info('Loading canopy dictionary...')
        with open(FLAGS.canopy2record_dict, 'rb') as fin:
            canopy2record = pickle.load(fin)

        with open(FLAGS.record2canopy_dict, 'rb') as fin:
            record2canopy = pickle.load(fin)

        logging.info('Loading rawinventor mentions....')
        rawinventor_cache_file = os.path.join(FLAGS.cache_dir, '%s.pkl' % os.path.basename(FLAGS.rawinventor))

        logging.info('Loading rawinventor mentions....')
        if os.path.exists(rawinventor_cache_file):
            logging.info('Using cache of rawinventor mentions %s', rawinventor_cache_file)
            with open('%s.pkl' % FLAGS.rawinventor, 'rb') as fin:
                record_dict = pickle.load(fin)
        else:
            logging.info('No cache of rawinventor mentions %s', '%s' % FLAGS.rawinventor)
            record_dict = dict()
            for m in load_inventor_mentions(FLAGS.rawinventor):
                record_dict[m.mention_id] = m
            with open('%s.pkl' % FLAGS.rawinventor, 'wb') as fout:
                pickle.dump(record_dict, fout)

        if need_to_load_dev:
            logging.info('Loading dev data ids and labels')

            canopies = set()
            point2label = dict()
            with open(FLAGS.dev_data, 'r') as fin:
                for line in fin:
                    splt = line.strip().split('\t')

                    # check to see we have the point id in the record dictionary
                    if splt[0] not in record_dict:
                        logging.info("[dev] missing record %s", splt[0])
                        continue

                    point2label[splt[0]] = splt[1]
                    pt_canopy = record2canopy[splt[0]]
                    canopies.add(pt_canopy)

            dev_set_tmp = dict()
            for canopy in canopies:
                pids = canopy2record[canopy]
                lbls = [point2label[x] if x in point2label else '-1' for x in pids]
                records = [record_dict[x] for x in pids]
                if len(records) >= FLAGS.min_dev_size and len(records) <= FLAGS.max_dev_size:
                    dev_set_tmp[canopy] = [pids, lbls, records]
            dev_datasets = dev_set_tmp

            dev_collection = [dev_datasets]
            with open(dev_cache_file, 'wb') as fin:
                pickle.dump(dev_collection, fin)

    logging.info('Number of dev canopies: %s', len(dev_datasets))

    encoding_model = InventorModel.from_flags(FLAGS)

    model = torch.load(FLAGS.model_path)

    logging.info('setting up features for dev datasets...')
    dev_sets = dict()
    for canopy, dataset in tqdm(dev_datasets.items(), 'dev data build'):
        pids, lbls, records = dataset[0], dataset[1], dataset[2]
        # logging.info('canopy %s - len(dataset) %s', canopy, len(records))
        dev_sets[canopy] = [pids, lbls, records, encoding_model.encode(records)]

    logging.info('dev eval using %s datasets', len(dev_sets))
    trees = []
    gold_clustering = []
    dataset_names = []
    for idx, (dataset_name, dataset) in enumerate(dev_sets.items()):
        pids, lbls, records, features = dataset[0], dataset[1], dataset[2], dataset[3]
        logging.info('Running on dev dataset %s of %s | %s with %s points', idx, len(dev_sets), dataset_name, len(pids))
        if len(pids) > 0:
            grinch = Agglom(model, features, num_points=len(pids))
            grinch.build_dendrogram_hac()
            trees.append(grinch)
            gold_clustering.extend(lbls)
            dataset_names.append(dataset_name)
    eval_ids = [i for i in range(len(gold_clustering)) if gold_clustering[i] != '-1']
    thresholds = [model.aux['threshold']]
    scores_per_threshold = []
    os.makedirs(os.path.join(outdir, 'dev'), exist_ok=True)
    dev_out_f = open(os.path.join(outdir, 'dev', 'dev.tsv'), 'w')
    pred_clustering = []
    for idx, t in enumerate(trees):
        fc = t.flat_clustering(model.aux['threshold'])
        pred_clustering.extend(['%s-%s' % (dataset_names[idx], x) for x in fc])
    metrics = eval_micro_pw_f1([pred_clustering[x] for x in eval_ids], [gold_clustering[x] for x in eval_ids])
    scores_per_threshold.append(metrics)
    logging.info('[dev] threshold %s | %s', model.aux['threshold'],
                 "|".join(['%s=%s' % (k, v) for k, v in metrics.items()]))

    for idx, t in enumerate(trees):
        dataset = dev_sets[dataset_names[idx]]
        pids, lbls, records, features = dataset[0], dataset[1], dataset[2], dataset[3]
        fc = t.flat_clustering(model.aux['threshold'])
        for j in range(len(records)):
            dev_out_f.write("%s\n" % records[j].pretty_tsv('%s-%s' % (dataset_names[idx], fc[j]), lbls[j]))
    dev_out_f.close()


if __name__ == "__main__":
    app.run(main)
