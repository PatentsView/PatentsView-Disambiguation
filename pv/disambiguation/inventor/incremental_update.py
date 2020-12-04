import os
import pickle

import numpy as np
import torch
import wandb
from absl import app
from absl import flags
from absl import logging
from grinch.agglom import Agglom
from grinch.multifeature_grinch import WeightedMultiFeatureGrinch
import configparser
from tqdm import tqdm

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


def run(config, loader, new_canopies, chunks, singleton_list,
              outdir, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(new_canopies), str(new_canopies[:5]))

    os.makedirs(outdir, exist_ok=True)
    statefile = os.path.join(outdir, job_name) + 'internals.pkl'

    # create a map from canopy to chunk
    canopy2chunks = dict()
    for idx, canopies in enumerate(chunks):
        for c in canopies:
            canopy2chunks[c] = str(idx)
    for s in singleton_list:
        canopy2chunks[s] = 'singletons'

    import collections

    new_canopies_by_chunk = collections.defaultdict(list)
    next_chunk = len(chunks)

    encoding_model = InventorModel.from_config(config)
    weight_model = torch.load(config['inventor']['model']).eval()

    for c in new_canopies:
        if c in canopy2chunks:
            new_canopies_by_chunk[canopy2chunks[c]].append(c)
        else:
            new_canopies_by_chunk[next_chunk].append(c)

    for this_chunk_id, this_chunk_canopies in new_canopies_by_chunk.items():
        if this_chunk_id != next_chunk:
            [grinch_trees, canopy2tree_id] = torch.load(statefile)
            for c in this_chunk_canopies:
                all_records = loader.load_canopies([c])
                all_pids = [x.uuid for x in all_records]
                all_lbls = -1 * np.ones(len(all_records))
                all_canopies = [c for c in all_lbls]
                features = encoding_model.encode(all_records)
                grinch_trees[canopy2tree_id[c]].update_and_insert(features)
                grinch_trees[canopy2tree_id[c]].clear_node_features()
                grinch_trees[canopy2tree_id[c]].points_set = False
            torch.save([grinch_trees, canopy2tree_id], statefile)
        else:
            grinch_trees = []
            canopy2tree_id = dict()
            for c in this_chunk_canopies:
                all_records = loader.load_canopies([c])
                all_pids = [x.uuid for x in all_records]
                all_lbls = -1 * np.ones(len(all_records))
                all_canopies = [c for c in all_lbls]
                features = encoding_model.encode(all_records)
                grinch = WeightedMultiFeatureGrinch(weight_model, features, len(all_pids))
                grinch_trees.append(grinch)
                canopy2tree_id[c] = len(grinch_trees) - 1
                grinch_trees[canopy2tree_id[c]].clear_node_features()
                grinch_trees[canopy2tree_id[c]].points_set = False
            torch.save([grinch_trees, canopy2tree_id], statefile)


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

    # Load the config files
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/inventor/run_clustering.ini', 'config/database_tables.ini'])
    logging.info('Config - %s', str(config))

    # A connection to the SQL database that will be used to load the inventor data.
    # Note that this will specifically load the
    # new canopies from config[config_type]['pregranted_canopies']
    # and config[config_type]['granted_canopies']
    loader = Loader.from_config(config, 'inventor')

    # note that this is a map from canopy_name to mention_id
    new_canopies = set(loader.pregranted_canopies.keys()).union(set(loader.granted_canopies.keys()))

    # setup the output dir
    outdir = os.path.join(config['inventor']['outprefix'], 'inventor', config['inventor']['run_id'])

    # determine where the canopies have been stored in chunks and singletons.
    with open(outdir + '/chunk2canopies.pkl', 'wb') as fout:
        [chunks, singletons] = pickle.load(fout)

    logging.info('Number of new canopies %s ', len(new_canopies))

    # run the job for the given batch
    run(config, loader, list(new_canopies), chunks, list(singletons), outdir,
              job_name='job-incremental')



if __name__ == "__main__":
    app.run(main)
