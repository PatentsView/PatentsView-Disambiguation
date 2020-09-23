import os
import pickle

import numpy as np
import wandb
from absl import app
from absl import flags
from absl import logging
from grinch.agglom import Agglom
from grinch.model import LinearAndRuleModel

from pv.disambiguation.assignee.load_name_mentions import Loader
from pv.disambiguation.assignee.model import AssigneeModel

FLAGS = flags.FLAGS

flags.DEFINE_string('assignee_canopies', 'data/assignee/assignee_mentions.canopies.pkl', '')
flags.DEFINE_string('assignee_mentions', 'data/assignee/assignee_mentions.records.pkl', '')
flags.DEFINE_string('assignee_name_model', 'data/assignee/permid/permid_vectorizer.pkl', '')

flags.DEFINE_string('model', 'exp_out/disambiguation-inventor-patentsview/solo/1rib1zt6/model-1000.torch', '')

flags.DEFINE_string('patent_titles', 'data/inventor/title_features.both.pkl', '')
flags.DEFINE_string('coinventors', 'data/inventor/coinventor_features.both.pkl', '')
flags.DEFINE_string('assignees', 'data/inventor/assignee_features.both.pkl', '')
flags.DEFINE_string('title_model', 'exp_out/sent2vec/patents/2020-05-10-15-08-42/model.bin', '')

flags.DEFINE_string('rawinventor', '/iesl/data/patentsview/2020-06-10/rawinventor.tsv', 'data path')
flags.DEFINE_string('outprefix', 'exp_out', 'data path')
flags.DEFINE_string('run_id', 'run_3', 'data path')

flags.DEFINE_string('dataset_name', 'patentsview', '')
flags.DEFINE_string('exp_name', 'disambiguation-inventor', '')

flags.DEFINE_string('base_id_file', '', '')

flags.DEFINE_integer('chunk_size', 10000, '')
flags.DEFINE_integer('chunk_id', 1000, '')
flags.DEFINE_integer('min_batch_size', 900, '')

flags.DEFINE_integer('max_canopy_size', 900, '')
flags.DEFINE_float('sim_threshold', 0.80, '')

logging.set_verbosity(logging.INFO)


def batched_canopy_process(datasets, model, encoding_model):
    logging.info('running on batch of %s with %s points', len(datasets), sum([len(x[1]) for x in datasets]))
    all_pids = []
    all_lbls = []
    all_records = []
    all_canopies = []
    for dataset_name, dataset in datasets:
        pids, lbls, records = dataset[0], dataset[1], dataset[2]
        all_canopies.extend([dataset_name for _ in range(len(pids))])
        all_pids.extend(pids)
        all_lbls.extend(lbls)
        all_records.extend(records)
    return run_on_batch(all_pids, all_lbls, all_records, all_canopies, model, encoding_model)


def run_on_batch(all_pids, all_lbls, all_records, all_canopies, model, encoding_model, canopy2predictions):
    features = encoding_model.encode(all_records)
    grinch = Agglom(model, features, num_points=len(all_pids), min_allowable_sim=0)
    grinch.build_dendrogram_hac()
    fc = grinch.flat_clustering(model.aux['threshold'])
    # import pdb
    # pdb.set_trace()
    for i in range(len(all_pids)):
        if all_canopies[i] not in canopy2predictions:
            canopy2predictions[all_canopies[i]] = [[], []]
        canopy2predictions[all_canopies[i]][0].append(all_pids[i])
        canopy2predictions[all_canopies[i]][1].append('%s-%s' % (all_canopies[i], fc[i]))
    return canopy2predictions


def needs_predicting(canopy_list, results, loader):
    return [c for c in canopy_list if c not in results]


def batcher(canopy_list, loader, min_batch_size=800):
    all_pids = []
    all_lbls = []
    all_records = []
    all_canopies = []
    for c in canopy_list:
        if len(all_pids) > min_batch_size:
            yield all_pids, all_lbls, all_records, all_canopies
            all_pids = []
            all_lbls = []
            all_records = []
            all_canopies = []
        records = loader.load(c)
        pids = [x.uuid for x in records]
        lbls = -1 * np.ones(len(records))
        all_canopies.extend([c for _ in range(len(pids))])
        all_pids.extend(pids)
        all_lbls.extend(lbls)
        all_records.extend(records)
    if len(all_pids) > 0:
        yield all_pids, all_lbls, all_records, all_canopies


def run_batch(canopy_list, outdir, loader, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    num_mentions_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    to_run_on = needs_predicting(canopy_list, results, None)
    logging.info('had results for %s, running on %s', len(canopy_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logging.info('already had all canopies completed! wrapping up here...')

    encoding_model = AssigneeModel.from_flags(FLAGS)
    weight_model = LinearAndRuleModel.from_encoding_model(encoding_model)
    weight_model.aux['threshold'] = 1 / (1 + FLAGS.sim_threshold)

    if to_run_on:
        for idx, (all_pids, all_lbls, all_records, all_canopies) in enumerate(
          batcher(to_run_on, loader, FLAGS.min_batch_size)):
            logging.info('[%s] run_batch %s - %s - processed %s mentions', job_name, idx, len(canopy_list),
                         num_mentions_processed)
            run_on_batch(all_pids, all_lbls, all_records, all_canopies, weight_model, encoding_model, results)
            if idx % 10 == 0:
                wandb.log({'computed': idx + FLAGS.chunk_id * FLAGS.chunk_size, 'num_mentions': num_mentions_processed})
                logging.info('[%s] caching results for job', job_name)
                with open(outfile, 'wb') as fin:
                    pickle.dump(results, fin)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)


def handle_singletons(canopy2predictions, singleton_canopies, loader):
    for s in singleton_canopies:
        ments = loader.load(s)
        assert len(ments) == 1
        canopy2predictions[s] = [[ments[0].uuid], [ments[0].uuid]]
    return canopy2predictions


def run_singletons(canopy_list, outdir, loader, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    num_mentions_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    to_run_on = needs_predicting(canopy_list, results, loader)
    logging.info('had results for %s, running on %s', len(canopy_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logging.info('already had all canopies completed! wrapping up here...')

    if to_run_on:
        handle_singletons(results, to_run_on, loader)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)


def main(argv):
    logging.info('Running clustering - %s ', str(argv))
    wandb.init(project="%s-%s" % (FLAGS.exp_name, FLAGS.dataset_name))
    wandb.config.update(flags.FLAGS)

    loader = Loader.from_flags(FLAGS)
    all_canopies = set(loader.assignee_canopies.keys())
    all_canopies = set([x for x in all_canopies if loader.num_records(x) < FLAGS.max_canopy_size])
    singletons = set([x for x in all_canopies if loader.num_records(x) == 1])
    all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                                 reverse=True)
    logging.info('Number of canopies %s ', len(all_canopies_sorted))
    logging.info('Number of singletons %s ', len(singletons))
    logging.info('Largest canopies - ')
    for c in all_canopies_sorted[:10]:
        logging.info('%s - %s records', c, loader.num_records(c))
    outdir = os.path.join(FLAGS.outprefix, 'assignee', FLAGS.run_id)
    num_chunks = int(len(all_canopies_sorted) / FLAGS.chunk_size)
    logging.info('%s num_chunks', num_chunks)
    logging.info('%s chunk_size', FLAGS.chunk_size)
    logging.info('%s chunk_id', FLAGS.chunk_id)
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)

    if FLAGS.chunk_id == 0:
        logging.info('Running singletons!!')
        run_singletons(list(singletons), outdir, job_name='job-singletons', loader=loader)

    run_batch(chunks[FLAGS.chunk_id], outdir, loader, job_name='job-%s' % FLAGS.chunk_id)


if __name__ == "__main__":
    app.run(main)
