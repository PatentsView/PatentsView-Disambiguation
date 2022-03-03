import configparser
import multiprocessing as mp
import os
import pickle

import numpy as np
import torch
from absl import app
from absl import logging
from grinch.agglom import Agglom

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


def run_on_batch(all_pids, all_lbls, all_records, all_canopies, model, encoding_model, canopy2predictions, canopy2tree,
                 trees, pids_list):
    """

    :param all_pids: Ids of the records we are running on
    :param all_lbls: Labels (or blank labels if they are not available)
    :param all_records: The records (i.e. features - python strings of the inventor name, assignee etc)
    :param all_canopies: The canopies of each record (right now this is always all the same canopy)
    :param model: linear scoring / similarity model which takes in two feature vectors and gives a similarity
    :param encoding_model: mapping from all_records gives you something that is input to model
    :param canopy2predictions: where results are stored
    :param canopy2tree: where results are stored
    :param trees: where results are stored
    :param pids_list: where results are stored
    :return:
    """
    # extracting features
    features = encoding_model.encode(all_records)
    if len(all_pids) > 1:
        # running clustering
        grinch = Agglom(model, features, num_points=len(all_pids))
        grinch.build_dendrogram_hac()
        fc = grinch.flat_clustering(model.aux['threshold'])
        # storing the state of the clustering for the incremental setting
        tree_id = len(trees)
        # store the tree that is build for this canopy
        trees.append(grinch)
        pids_list.append(all_pids)
        for i in range(len(all_pids)):
            # record mapping from canopy to the tree id
            canopy2tree[all_canopies[i]] = tree_id
            if all_canopies[i] not in canopy2predictions:
                canopy2predictions[all_canopies[i]] = [[], []]
                canopy2tree[all_canopies[i]] = tree_id
            # save predictions (used in the non incremental setting)
            canopy2predictions[all_canopies[i]][0].append(all_pids[i])
            canopy2predictions[all_canopies[i]][1].append('%s-%s' % (all_canopies[i], fc[i]))
        return canopy2predictions
    else:
        raise Exception('Must have non-singleton canopies')
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


def run_batch(config, canopy_list, outdir, chunk_id, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    canopy2tree_id = dict()
    tree_list = []
    pids_list = []
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
            run_on_batch(all_pids, all_lbls, all_records, all_canopies, weight_model, encoding_model, results,
                         canopy2tree_id, tree_list, pids_list)
            num_mentions_processed += len(all_pids)
            num_canopies_processed += np.unique(all_canopies).shape[0]
            if idx % 10 == 0:
                logging.info(
                    {'computed': idx + int(chunk_id) * int(config['inventor']['chunk_size']),
                     'num_mentions': num_mentions_processed,
                     'num_canopies_processed': num_canopies_processed})
            #     logging.info('[%s] caching results for job', job_name)
            #     with open(outfile, 'wb') as fin:
            #         pickle.dump(results, fin)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)

    # logging.info('Beginning to save all tree structures....')
    # grinch_trees = []
    # for idx,t in tqdm(enumerate(tree_list), total=len(tree_list)):
    #     grinch = WeightedMultiFeatureGrinch.from_agglom(t, pids_list[idx])
    #     grinch.prepare_for_save()
    #     grinch_trees.append(grinch)
    # torch.save([grinch_trees, canopy2tree_id], outstatefile)


def run_singletons(config, loader, singleton_list, outdir, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(singleton_list), str(singleton_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    statefile = os.path.join(outdir, job_name) + 'internals.pkl'

    import collections

    new_canopies_by_chunk = collections.defaultdict(list)

    encoding_model = InventorModel.from_config(config)
    weight_model = torch.load(config['inventor']['model']).eval()

    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    num_mentions_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    loader = Loader.from_config(config, 'inventor')

    to_run_on = needs_predicting(singleton_list, results, loader)
    logging.info('had results for %s, running on %s', len(singleton_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logging.info('already had all canopies completed! wrapping up here...')

    for c in singleton_list:
        new_canopies_by_chunk['singletons'].append(c)

    # for this_chunk_id, this_chunk_canopies in tqdm(new_canopies_by_chunk.items()):
    #     grinch_trees = []
    #     canopy2tree_id = dict()
    #     for c in this_chunk_canopies:
    #         all_records = loader.load_canopies([c])
    #         if len(all_records) > 0:
    #             all_pids = [x.uuid for x in all_records]
    #             all_lbls = -1 * np.ones(len(all_records))
    #             all_canopies = [c for c in all_lbls]
    #             features = encoding_model.encode(all_records)
    #             grinch = WeightedMultiFeatureGrinch(weight_model, features, len(all_pids))
    #             grinch.pids = all_pids
    #             grinch.prepare_for_save()
    #             grinch_trees.append(grinch)
    #             canopy2tree_id[c] = len(grinch_trees)-1
    #     torch.save([grinch_trees, canopy2tree_id], statefile)

    if to_run_on:
        handle_singletons(results, to_run_on, loader)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)


def run_clustering(config):
    # A connection to the SQL database that will be used to load the inventor data.
    loader = Loader.from_config(config)

    # Find all of the canopies in the entire dataset.
    all_canopies = set(loader.pregranted_canopies.keys()).union(set(loader.granted_canopies.keys()))
    singletons = set([x for x in all_canopies if loader.num_records(x) == 1])
    all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                                 reverse=True)

    # Find some stats of the data before chunking it
    logging.info('Number of canopies %s ', len(all_canopies_sorted))
    logging.info('Number of singletons %s ', len(singletons))
    logging.info('Largest canopies - ')
    for c in all_canopies_sorted[:10]:
        logging.info('%s - %s records', c, loader.num_records(c))

    # setup the output dir
    outdir = config['inventor']['clustering_output_folder']

    # the number of chunks based on the specified chunksize
    num_chunks = max(1, int(len(all_canopies_sorted) / int(config['inventor']['chunk_size'])))

    logging.info('%s num_chunks', num_chunks)
    logging.info('%s chunk_size', int(config['inventor']['chunk_size']))

    # chunk all of the data by canopy
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)
    pool = mp.Pool()
    argument_list = [(config, chunks[x], outdir, x, 'job-%s' % x) for x in range(0, num_chunks)]
    dev_null = [
        n for n in pool.starmap(
            run_batch, argument_list)
    ]
    # chunk 0 will write out the meta data and singleton information
    logging.info('Saving chunk to canopy map')
    with open(outdir + '/chunk2canopies.pkl', 'wb') as fout:
        pickle.dump([chunks, list(singletons)], fout)

    logging.info('Running singletons!!')
    num_singleton_chunks = max(1, int(len(singletons) / int(config['inventor']['chunk_size'])))
    # chunk all of the data by canopy
    singleton_chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(singletons):
        singleton_chunks[idx % num_singleton_chunks].append(c)
    pool = mp.Pool()
    argument_list = [(config, singleton_chunks[x], outdir, x, 'singleton-job-%s' % x) for x in range(0, num_singleton_chunks)]
    dev_null = [
        n for n in pool.starmap(
            run_singletons, argument_list)
    ]

def main(argv):
    logging.info('Running clustering - argv =  %s ', str(argv))

    # Load the config files
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/inventor/run_clustering.ini', 'config/database_tables.ini'])
    logging.info('Config - %s', str(config))
    run_clustering(config)


if __name__ == "__main__":
    app.run(main)
