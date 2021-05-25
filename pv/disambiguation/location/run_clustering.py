import collections
import os
import pickle

import numpy as np
import wandb
from absl import app
from absl import logging
from grinch.model import LinearAndRuleModel
import configparser

from pv.disambiguation.location.load import Loader
from pv.disambiguation.location.model import LocationAgglom, LocationModelWithApps

logging.set_verbosity(logging.INFO)


def run_on_batch(config, all_pids, all_lbls, all_records, all_canopies, model, encoding_model, canopy2predictions):
    features = encoding_model.encode(all_records)
    grinch = LocationAgglom(model, features, num_points=len(all_pids),relaxed_sim_threshold=float(config['location']['relaxed_sim_threshold']))
    grinch.build_dendrogram_hac()
    fc = grinch.flat_clustering(model.aux['threshold'])
    canopy2cluster2mentions = dict()
    for i in range(len(all_pids)):
        if all_canopies[i] not in canopy2cluster2mentions:
            canopy2cluster2mentions[all_canopies[i]] = collections.defaultdict(list)
        canopy2cluster2mentions[all_canopies[i]]['%s-%s' % (all_canopies[i], fc[i])].append(all_records[i])
    for canopy, cluster2mentions in canopy2cluster2mentions.items():
        if canopy not in canopy2predictions:
            canopy2predictions[canopy] = [[], []]
        for c, ments in cluster2mentions.items():
            eid = entity_id(ments)
            for m in ments:
                for mid in m.mention_ids:
                    canopy2predictions[canopy][0].append(mid)
                    canopy2predictions[canopy][1].append(eid)
    return canopy2predictions


def needs_predicting(canopy_list, results, loader):
    return [c for c in canopy_list if c not in results]


def batcher(canopy_list, loader, min_batch_size=1):
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


def run_batch(config, canopy_list, outdir, loader, job_name='disambig'):
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

    encoding_model = LocationModelWithApps.from_config(config)
    weight_model = LinearAndRuleModel.from_encoding_model(encoding_model)
    weight_model.aux['threshold'] = 0.5 + 1e-5
    # loader = Loader.from_config(config)

    if to_run_on:
        for idx, (all_pids, all_lbls, all_records, all_canopies) in enumerate(
          batcher(to_run_on, loader, int(config['location']['min_batch_size']))):
            logging.info('[%s] run_batch %s - %s - processed %s mentions', job_name, idx, len(canopy_list),
                         num_mentions_processed)
            run_on_batch(config,all_pids, all_lbls, all_records, all_canopies, weight_model, encoding_model, results)
            # if idx % 10000 == 0:
            #     wandb.log({'computed': idx + int(config['location']['chunk_id']) * int(config['location']['chunk_size']), 'num_mentions': num_mentions_processed})
            #     logging.info('[%s] caching results for job', job_name)
            #     with open(outfile, 'wb') as fin:
            #         pickle.dump(results, fin)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)


def entity_id(cluster_of_records):
    from collections import defaultdict
    counts = defaultdict(int)
    in_db = set()
    for r in cluster_of_records:
        r_str = '%s|%s|%s' % (r._canonical_city, r._canonical_state, r._canonical_country)
        if r._in_database == 1:
            in_db.add(r_str)
        counts[r_str] += 1
    if in_db:
        return max(in_db, key=lambda x: counts[x])
    else:
        return max(counts, key=lambda x: counts[x])


def handle_singletons(canopy2predictions, singleton_canopies, loader):
    for s in singleton_canopies:
        ments = loader.name_mentions[s]
        assert len(ments) == 1
        canopy2predictions[s] = [[], []]
        eid = entity_id(ments)
        for m in ments[0].mention_ids:
            canopy2predictions[s][0].append(m)
            canopy2predictions[s][1].append(eid)
    return canopy2predictions


def run_singletons(config,canopy_list, outdir, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    logging.info('loader starting...')

    loader = Loader.from_config(config)
    logging.info('loader ending...')

    to_run_on = needs_predicting(canopy_list, results, loader)
    logging.info('had results for %s, running on %s', len(canopy_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logging.info('already had all canopies completed! wrapping up here...')

    if to_run_on:
        handle_singletons(results, to_run_on, loader)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)


def main(argv):
    logging.info('Running location clustering - %s ', str(argv))
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/location/run_clustering.ini',
                 'config/database_tables.ini'])

    if len(argv) > 1:
        logging.info('Using cmd line arg for chunk id %s' % argv[1])
        config['location']['chunk_id'] = argv[1]

    loader = Loader.from_config(config)
    all_canopies = set(loader.name_mentions.keys())
    singletons = set([x for x in all_canopies if loader.num_records(x) == 1])
    all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                                 reverse=True)
    logging.info('Number of canopies %s ', len(all_canopies_sorted))
    logging.info('Number of singletons %s ', len(singletons))
    logging.info('Largest canopies - ')
    for c in all_canopies_sorted[:10]:
        logging.info('%s - %s records', c, loader.num_records(c))
    outdir = os.path.join(config['location']['outprefix'], 'location', config['location']['run_id'])
    num_chunks = int(len(all_canopies_sorted) / int(config['location']['chunk_size']))
    logging.info('%s num_chunks', num_chunks)
    logging.info('%s chunk_size', int(config['location']['chunk_size']))
    logging.info('%s chunk_id', (config['location']['chunk_id']))
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)

    if (config['location']['chunk_id']) == 'singletons':
        logging.info('Running singletons!!')
        run_singletons(config, list(singletons), outdir, job_name='job-singletons')
    else:
        # for chunk_id in range(0, num_chunks):
        print("Starting Chunk ID: {cid}".format(cid=config['location']['chunk_id']))
        run_batch(config, chunks[int(config['location']['chunk_id'])], outdir, loader, job_name='job-%s' % int(int(config['location']['chunk_id'])))


if __name__ == "__main__":
    app.run(main)
