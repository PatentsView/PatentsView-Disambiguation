import os
import pickle

import wandb
from absl import app
from absl import flags
from absl import logging
from grinch.agglom import Agglom
from grinch.model import LinearAndRuleModel
from grinch.train_model import Trainer
from tqdm import tqdm

from pv.disambiguation.core import load_inventor_mentions
from pv.disambiguation.inventor.model import InventorModel
from pv.disambiguation.inventor.run_clustering import FLAGS

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
flags.DEFINE_integer('num_samples', 100000, '')
flags.DEFINE_integer('num_negatives', 5, '')
flags.DEFINE_integer('batch_size', 10, '')
flags.DEFINE_integer('dev_every', 1000, '')
flags.DEFINE_integer('epochs', 3, '')
flags.DEFINE_float('margin', 0.1, '')
flags.DEFINE_float('lr', 0.0001, '')

flags.DEFINE_integer('num_thresholds', 1000, '')
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
    train_cache_file = os.path.join(FLAGS.cache_dir, 'train-%s.pkl' % os.path.basename(FLAGS.training_data))
    dev_cache_file = os.path.join(FLAGS.cache_dir, 'dev-%s.pkl' % os.path.basename(FLAGS.dev_data))
    if os.path.exists(train_cache_file):
        logging.info('Using cached training data! %s', train_cache_file)
        with open(train_cache_file, 'rb') as fin:
            training_collection = pickle.load(fin)
            all_training_pids, all_training_labels, all_training_records, train_datasets = training_collection
    else:
        need_to_load_train = True

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

        if need_to_load_train:
            logging.info('Loading training data ids and labels')
            training_point_ids = []
            training_labels = []
            train_datasets = dict()
            with open(FLAGS.training_data, 'r') as fin:
                for line in fin:
                    splt = line.strip().split('\t')

                    # check to see we have the point id in the record dictionary
                    if splt[0] not in record_dict:
                        logging.info("[train] missing record %s", splt[0])
                        continue

                    training_point_ids.append(splt[0])
                    training_labels.append(splt[1])
                    pt_canopy = record2canopy[splt[0]]
                    if pt_canopy not in train_datasets:
                        train_datasets[pt_canopy] = [[], []]
                    train_datasets[pt_canopy][0].append(splt[0])
                    train_datasets[pt_canopy][1].append(splt[1])
            training_set_tmp = dict()
            all_training_pids = []
            all_training_records = []
            all_training_labels = []
            for canopy, dataset in train_datasets.items():
                pids, lbls = dataset[0], dataset[1]
                records = [record_dict[x] for x in pids]
                all_training_pids.extend(pids)
                all_training_records.extend(records)
                all_training_labels.extend(lbls)
                training_set_tmp[canopy] = [pids, lbls, records]
            train_datasets = training_set_tmp

            training_collection = [all_training_pids, all_training_labels, all_training_records, train_datasets]
            with open(train_cache_file, 'wb') as fin:
                pickle.dump(training_collection, fin)

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

    logging.info('Number of train canopies: %s', len(train_datasets))
    logging.info('Number of dev canopies: %s', len(dev_datasets))

    encoding_model = InventorModel.from_flags(FLAGS)

    model = LinearAndRuleModel.from_encoding_model(encoding_model)

    logging.info('building features for the training model...')
    logging.info('len(all_training_records) = %s', len(all_training_records))

    features = encoding_model.encode(all_training_records)
    # grinch = WeightedMultiFeatureGrinch(model, features, num_points=len(all_training_pids), max_nodes=50000)
    grinch = Agglom(model, features, num_points=len(all_training_pids))
    # from tqdm import tqdm
    # for i in tqdm(range(len(all_training_pids)), 'Initializing Grinch w/ All Points.'):
    #     grinch.add_pt(i)

    logging.info('setting up features for dev datasets...')
    dev_sets = dict()
    for canopy, dataset in tqdm(dev_datasets.items(), 'dev data build'):
        pids, lbls, records = dataset[0], dataset[1], dataset[2]
        # logging.info('canopy %s - len(dataset) %s', canopy, len(records))
        dev_sets[canopy] = [pids, lbls, records, encoding_model.encode(records)]

    trainer = Trainer(outdir=outdir,
                      model=model,
                      encoding_model=encoding_model,
                      grinch=grinch,
                      pids=all_training_pids, labels=all_training_labels, points=all_training_records,
                      dev_data=dev_sets, num_samples=FLAGS.num_samples, num_negatives=FLAGS.num_negatives,
                      batch_size=FLAGS.batch_size, dev_every=FLAGS.dev_every, epochs=FLAGS.epochs, lr=FLAGS.lr,
                      weight_decay=0.0, margin=FLAGS.margin, num_thresholds=FLAGS.num_thresholds,
                      max_dev_size=FLAGS.max_dev_canopy_size,
                      max_dev_canopies=FLAGS.max_num_dev_canopies)
    trainer.train()


if __name__ == "__main__":
    app.run(main)
