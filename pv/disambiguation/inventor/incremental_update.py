import os
import pickle

import numpy as np
import torch
from absl import app
from absl import flags
from absl import logging
from grinch.agglom import Agglom
from grinch.multifeature_grinch import WeightedMultiFeatureGrinch
import configparser
import time
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


def run_chunk(args):
    return run_chunk_(args[0], args[1], args[2], args[3])

def run_chunk_(config, outdir, this_chunk_id, this_chunk_canopies):
    updatedfile = os.path.join(outdir, 'job-%s' % this_chunk_id) + 'internals-updated.pkl'
    if os.path.exists(updatedfile):
        return

    encoding_model = InventorModel.from_config(config)
    weight_model = torch.load(config['inventor']['model']).eval()
    loader = Loader.from_config(config, 'inventor')

    statefile = os.path.join(outdir, 'job-%s' % this_chunk_id) + 'internals.pkl'
    if os.path.exists(statefile):
        logging.info('chunk: %s - process %s', this_chunk_id, len(this_chunk_canopies))

        # Load the grinch trees
        logging.info('starting load....')
        [grinch_trees, canopy2tree_id] = torch.load(statefile)
        logging.info('end load....')

        # load the old predictions
        predfile = os.path.join(outdir, 'job-%s' % this_chunk_id) + '.pkl'
        with open(predfile, 'rb') as fin:
            canopy2predictions = pickle.load(fin)

        chunk_st = time.time()
        for idx, c in enumerate(this_chunk_canopies):
            logging.info('Working on canopy %s', c)
            logging.info('chunk: %s - processed %s of %s - elapsed %s', this_chunk_id, idx, len(this_chunk_canopies),
                         time.time() - chunk_st)

            all_records = loader.load_canopies([c])
            all_pids = [x.uuid for x in all_records]
            all_lbls = -1 * np.ones(len(all_records))
            all_canopies = [c for c in all_lbls]
            features = encoding_model.encode(all_records)
            logging.info('Tree ID %s', canopy2tree_id[c])
            if canopy2tree_id[c] is None:
                logging.info('we were missing a tree for canopy %s running now!', c)
                grinch = WeightedMultiFeatureGrinch(weight_model, features, len(all_pids), pids=all_pids)
                grinch.perform_graft = False
                grinch.build_dendrogram()
                fc = grinch.flat_clustering(weight_model.aux['threshold'])
                grinch.clear_node_features()
                grinch.points_set = False
                canopy2predictions[c] = [[], []]
                logging.info('len(all_pids) %s', len(all_pids))
                logging.info('grinch.num_points %s', grinch.num_points)
                logging.info('grinch.pids %s', len(grinch.pids))
                logging.info('fc %s', str(fc))
                for i in range(grinch.num_points):
                    canopy2predictions[c][0].append(grinch.pids[i])
                    canopy2predictions[c][1].append('%s-%s' % (c, fc[i]))
                grinch_trees.append(grinch)
                canopy2tree_id[c] = len(grinch_trees)

            else:
                grinch = grinch_trees[canopy2tree_id[c]]
                st_up = time.time()
                grinch.perform_graft = False
                grinch.update_and_insert(features, all_pids)
                en_up = time.time()
                logging.info('[update_and_insert] time %s', en_up - st_up)

                st_fc = time.time()
                fc = grinch.flat_clustering(weight_model.aux['threshold'])
                en_fc = time.time()
                logging.info('[flat_clustering] time %s', en_fc - st_fc)

                st_cl = time.time()
                grinch.clear_node_features()
                en_cl = time.time()
                logging.info('[clear_node_features] time %s', en_cl - st_cl)

                st_sav_pred = time.time()
                grinch.points_set = False
                canopy2predictions[c] = [[], []]
                for i in range(grinch.num_points):
                    canopy2predictions[c][0].append(grinch.pids[i])
                    canopy2predictions[c][1].append('%s-%s' % (c, fc[i]))
                en_sav_pred = time.time()
                logging.info('[canopy2predictions save] time %s', en_sav_pred - st_sav_pred)

        statefile = os.path.join(outdir, 'job-%s' % this_chunk_id) + 'internals-updated.pkl'
        predfile = os.path.join(outdir, 'job-%s' % this_chunk_id) + '-updated.pkl'

        torch.save([grinch_trees, canopy2tree_id], statefile)
        with open(predfile, 'wb') as fin:
            pickle.dump(canopy2predictions, fin)
    else:
        grinch_trees = []
        canopy2tree_id = dict()
        canopy2predictions = dict()
        for c in this_chunk_canopies:
            all_records = loader.load_canopies([c])
            all_pids = [x.uuid for x in all_records]
            all_lbls = -1 * np.ones(len(all_records))
            all_canopies = [c for c in all_lbls]
            features = encoding_model.encode(all_records)
            grinch = WeightedMultiFeatureGrinch(weight_model, features, len(all_pids))
            grinch.perform_graft = False
            grinch.build_dendrogram()
            grinch_trees.append(grinch)
            canopy2tree_id[c] = len(grinch_trees) - 1
            grinch_trees[canopy2tree_id[c]].clear_node_features()
            grinch_trees[canopy2tree_id[c]].points_set = False
            fc = grinch.flat_clustering(weight_model.aux['threshold'])
            canopy2predictions[c] = [[], []]
            for i in range(grinch.num_points):
                canopy2predictions[all_canopies[i]][0].append(grinch.all_pids[i])
                canopy2predictions[c][1].append('%s-%s' % (c, fc[i]))

        statefile = os.path.join(outdir, 'job-%s' % this_chunk_id) + 'internals-updated.pkl'
        predfile = os.path.join(outdir, 'job-%s' % this_chunk_id) + '-updated.pkl'
        torch.save([grinch_trees, canopy2tree_id], statefile)
        with open(predfile, 'wb') as fin:
            pickle.dump(canopy2predictions, fin)


def run(config, loader, new_canopies, chunks, singleton_list,
              outdir, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(new_canopies), str(new_canopies[:5]))

    os.makedirs(outdir, exist_ok=True)

    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    outstatefile = os.path.join(outdir, job_name) + 'internals.pkl'

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

    for c in new_canopies:
        if c in canopy2chunks:
            new_canopies_by_chunk[canopy2chunks[c]].append(c)
        else:
            new_canopies_by_chunk[next_chunk].append(c)

    logging.info('number of chunks to process %s', len(new_canopies_by_chunk))
    total_canopies_to_process = 0
    for this_chunk_id, this_chunk_canopies in new_canopies_by_chunk.items():
        logging.info('chunk: %s - process %s', this_chunk_id, len(this_chunk_canopies))
        total_canopies_to_process += len(this_chunk_canopies)

    logging.info('number of canopies to process %s', total_canopies_to_process)
    # yn = input('continue? ')
    # if yn == 'n':
    #     import sys
    #     sys.exit(0)

    chunk_st = time.time()

    from pathos.multiprocessing import ProcessingPool
    num_cores = int(config['inventor']['ncpus'])

    logging.info('using n cores %s', num_cores)

    batches = [(config, outdir, this_chunk_id, this_chunk_canopies) for this_chunk_id, this_chunk_canopies in new_canopies_by_chunk.items()]
    results = [n for n in ProcessingPool(num_cores).imap(run_chunk, batches)]

    # for this_chunk_id, this_chunk_canopies in tqdm(new_canopies_by_chunk.items(), 'chunks'):
    #     run_chunk_(config, outdir, this_chunk_id, this_chunk_canopies)



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
    config.read(['config/database_config.ini',
                 'config/inventor/run_incremental.ini',
                 'config/database_tables.ini'])
    logging.info('Config - %s', str(config))

    # A connection to the SQL database that will be used to load the inventor data.
    # Note that this will specifically load the
    # new canopies from config[config_type]['pregranted_canopies']
    # and config[config_type]['granted_canopies']
    loader = Loader.from_config(config, 'inventor')

    # note that this is a map from canopy_name to mention_id
    new_canopies = set(loader.pregranted_canopies.keys()).union(set(loader.granted_canopies.keys()))
    # new_canopies = [x for x in new_canopies if loader.num_records(x) == 12]

    # setup the output dir
    outdir = os.path.join(config['inventor']['outprefix'], 'inventor', config['inventor']['run_id'])

    # determine where the canopies have been stored in chunks and singletons.
    with open(outdir + '/chunk2canopies.pkl', 'rb') as fout:
        [chunks, singletons] = pickle.load(fout)

    logging.info('Number of new canopies %s ', len(new_canopies))

    s = time.time()
    # run the job for the given batch
    run(config, loader, list(new_canopies), chunks, list(singletons), outdir,
              job_name='job-incremental')
    t = time.time()

    logging.info('total time for incremental update: %s', t-s)



if __name__ == "__main__":
    app.run(main)
