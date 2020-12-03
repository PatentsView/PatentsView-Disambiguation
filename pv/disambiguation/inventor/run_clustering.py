import os
import pickle

import numpy as np
import torch
import wandb
from absl import app
from absl import flags
from absl import logging
from grinch.agglom import Agglom
import configparser

from pv.disambiguation.inventor.load_mysql import Loader
from pv.disambiguation.inventor.model import InventorModel

logging.set_verbosity(logging.INFO)

def handle_singletons(canopy2predictions, singleton_canopies, loader):
    for s in singleton_canopies:
        pgids, gids = loader.pregranted_canopies[s], loader.granted_canopies[s]
        assert len(pgids) == 1 or len(gids) == 1
        if gids:
            canopy2predictions[s] = [[gids[0]], [gids[0]]]
        else:
            canopy2predictions[s] = [[pgids[0]], [pgids[0]]]
    return canopy2predictions


def run_on_batch(all_pids, all_lbls, all_records, all_canopies, model, encoding_model, canopy2predictions, canopy2tree, trees):
    features = encoding_model.encode(all_records)
    if len(all_pids) > 1:
        grinch = Agglom(model, features, num_points=len(all_pids))
        grinch.build_dendrogram_hac()
        fc = grinch.flat_clustering(model.aux['threshold'])
        tree_id = len(trees)
        trees.append(grinch)
        for i in range(len(all_pids)):
            if all_canopies[i] not in canopy2predictions:
                canopy2predictions[all_canopies[i]] = [[], []]
                canopy2tree[all_canopies[i]] = tree_id
            canopy2predictions[all_canopies[i]][0].append(all_pids[i])
            canopy2predictions[all_canopies[i]][1].append('%s-%s' % (all_canopies[i], fc[i]))
        return canopy2predictions
    else:
        fc = [0]
        for i in range(len(all_pids)):
            if all_canopies[i] not in canopy2predictions:
                canopy2predictions[all_canopies[i]] = [[], []]
                canopy2tree[all_canopies[i]] = None
            canopy2predictions[all_canopies[i]][0].append(all_pids[i])
            canopy2predictions[all_canopies[i]][1].append('%s-%s' % (all_canopies[i], fc[i]))
        return canopy2predictions


def needs_predicting(canopy_list, results, loader):
    res = []
    for c in canopy_list:
        if c not in results or (c in canopy_list and len(results[c]) != loader.num_records(c)):
            res.append(c)
    return res


def form_canopy_groups(canopy_list, loader, min_batch_size=800):
    size_pairs = [(c, loader.num_records(c)) for c in canopy_list]
    batches = [[]]
    batch_sizes = [0]
    batch_id = 0
    for c, s in size_pairs:
        if batch_sizes[-1] < min_batch_size:
            batch_sizes[-1] += s
            batches[-1].append(c)
        else:
            batches.append([c])
            batch_sizes.append(s)
    return batches, batch_sizes, dict(size_pairs)


def batch(canopy_list, loader, min_batch_size=800):
    batches, batch_sizes, sizes = form_canopy_groups(canopy_list, loader, min_batch_size)
    for batch, batch_size in zip(batches, batch_sizes):
        if batch_size > 0:
            all_records = loader.load_canopies(batch)
            all_pids = [x.uuid for x in all_records]
            all_lbls = -1 * np.ones(len(all_records))
            all_canopies = []
            for c in batch:
                all_canopies.extend([c for _ in range(sizes[c])])
            yield all_pids, all_lbls, all_records, all_canopies


def run_batch(config, canopy_list, outdir, job_name='disambig', singletons=None):
    logging.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    canopy2tree_id = dict()
    tree_list = []
    outfile = os.path.join(outdir, job_name) + '.pkl'
    outstatefile = os.path.join(outdir, job_name) + 'internals.pkl'
    num_mentions_processed = 0
    num_canopies_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    loader = Loader.from_config(config, 'inventor')

    to_run_on = needs_predicting(canopy_list, results, loader)
    logging.info('had results for %s, running on %s', len(canopy_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logging.info('already had all canopies completed! wrapping up here...')

    encoding_model = InventorModel.from_config(config)
    weight_model = torch.load(config['inventor']['model']).eval()

    if to_run_on:
        for idx, (all_pids, all_lbls, all_records, all_canopies) in enumerate(
          batch(to_run_on, loader, int(config['inventor']['min_batch_size']))):
            logging.info('[%s] run_batch %s - %s of %s - processed %s mentions', job_name, idx, num_canopies_processed,
                         len(canopy_list),
                         num_mentions_processed)
            run_on_batch(all_pids, all_lbls, all_records, all_canopies, weight_model, encoding_model, results, canopy2tree_id, tree_list)
            num_mentions_processed += len(all_pids)
            num_canopies_processed += np.unique(all_canopies).shape[0]
            if idx % 10 == 0:
                wandb.log({'computed': idx + int(config['inventor']['chunk_id']) * int(config['inventor']['chunk_size']),
                           'num_mentions': num_mentions_processed,
                           'num_canopies_processed': num_canopies_processed})
            #     logging.info('[%s] caching results for job', job_name)
            #     with open(outfile, 'wb') as fin:
            #         pickle.dump(results, fin)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)

    torch.save([tree_list, canopy2tree_id], outstatefile)


def run_singletons(config, canopy_list, outdir, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    num_mentions_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    loader = Loader.from_config(config, 'inventor')

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
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/inventor/run_clustering.ini'])

    logging.info('Config - %s', str(config))

    loader = Loader.from_config(config, 'inventor')

    all_canopies = set(loader.pregranted_canopies.keys()).union(set(loader.granted_canopies.keys()))
    singletons = set([x for x in all_canopies if loader.num_records(x) == 1])
    all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                                 reverse=True)

    logging.info('Number of canopies %s ', len(all_canopies_sorted))
    logging.info('Number of singletons %s ', len(singletons))
    logging.info('Largest canopies - ')
    for c in all_canopies_sorted[:10]:
        logging.info('%s - %s records', c, loader.num_records(c))
    outdir = os.path.join(config['inventor']['outprefix'], 'inventor', config['inventor']['run_id'])
    num_chunks = int(len(all_canopies_sorted) / int(config['inventor']['chunk_size']))
    logging.info('%s num_chunks', num_chunks)
    logging.info('%s chunk_size', int(config['inventor']['chunk_size']))
    logging.info('%s chunk_id', int(config['inventor']['chunk_id']))
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)

    if int(config['inventor']['chunk_id']) == 0:
        logging.info('Running singletons!!')
        run_singletons(config, list(singletons), outdir, job_name='job-singletons')

        logging.info('Saving chunk to canopy map')
        with open(outdir + '/chunk2canopies.pkl', 'wb') as fout:
            pickle.dump(chunks, fout)

    run_batch(config, chunks[int(config['inventor']['chunk_id'])], outdir,
              job_name='job-%s' % int(config['inventor']['chunk_id']))



if __name__ == "__main__":
    app.run(main)
