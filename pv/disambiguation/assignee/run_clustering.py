import configparser
import billiard as mp
import os
import pickle
import typing
import numpy as np
import pandas as pd
import torch
from absl import app
import logging
from grinch.agglom import Agglom
from grinch.model import LinearAndRuleModel
from grinch.multifeature_grinch import WeightedMultiFeatureGrinch
from scipy import sparse
from tqdm import tqdm

from pv.disambiguation.assignee.assignee_analyzer import load_assignee_analyzer_configuration, \
    assignee_abbreviation_file, assignee_correction_file, assignee_stopphrase_file
from pv.disambiguation.assignee.load_name_mentions import Loader
from pv.disambiguation.assignee.model import AssigneeModel

logger = logging.getLogger('Assignee Disambiguation')
logger.setLevel(logging.INFO)


def batched_canopy_process(datasets, model, encoding_model):
    logger.info('running on batch of %s with %s points', len(datasets), sum([len(x[1]) for x in datasets]))
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


def lookup_cluster_labels(idx, names, label_set_1, label_set_2):
    current_names = []
    if idx < len(names):
        current_names.append(names[int(idx)])
    else:
        offset = int(idx) - len(names)
        current_names = label_set_1[offset] + label_set_2[offset]
    return current_names


def lookup_names(Z, nms):
    cluster_1_labels = []
    cluster_2_labels = []
    distances = []
    elements = []
    cluster_1_ids = []
    cluster_2_ids = []
    for record in Z:
        cluster_1_idx = record[0]
        cluster_2_idx = record[1]
        cluster_1_labels.append(lookup_cluster_labels(cluster_1_idx, nms, cluster_1_labels, cluster_2_labels))
        cluster_2_labels.append(lookup_cluster_labels(cluster_2_idx, nms, cluster_1_labels, cluster_2_labels))
        distances.append(record[2])
        elements.append(record[3])
        cluster_1_ids.append(cluster_1_idx)
        cluster_2_ids.append(cluster_2_idx)
    import pandas as pd
    return pd.DataFrame(
        {'set_1_labels': cluster_1_labels, 'set_2_labels': cluster_2_labels, 'distance': distances,
         'elements': elements, 'cluster_1_id': cluster_1_ids, 'cluster_2_id': cluster_2_ids}
    )


def write_feature_debug(debug_location, columns, feature):
    sparse.save_npz(f"{debug_location}/Feature_{feature[0]}.npz", feature[3])
    with open(f"{debug_location}/Label_{feature[0]}_names.csv", "w") as fp:
        for col in columns:
            fp.write(f"{col}\n")
def run_on_batch(all_pids, all_lbls, all_records, all_canopies, model, encoding_model, canopy2predictions, canopy2tree,
                 trees, pids_list, canopy_list, job_name):
    debug_folder_name = f"debug/{job_name}/{batch}/"
    import os
    os.makedirs(debug_folder_name, exist_ok=True)
    nms = [m.normalized_most_frequent for m in all_records]
    with open(f"{debug_folder_name}/Label_Record_names.csv", "w") as fp:
        for name in nms:
            fp.write(f"{name}\n")
    features = encoding_model.encode(all_records)
    for feature, ml in zip(features, encoding_model.feature_list):
        write_feature_debug(debug_location=debug_folder_name, columns=ml.model.get_feature_names_out(),
                            feature=feature)
    grinch = Agglom(model, features, num_points=len(all_pids), min_allowable_sim=0)
    grinch.build_dendrogram_hac()
    # pickle.dump(grinch.Z, open('Z_{jn}.pkl'.format(jn=job_name), 'wb'))
    Z_frame = lookup_names(grinch.Z, nms)
    Z_frame = Z_frame[Z_frame.elements < 50]
    z_name = f"{debug_folder_name}/Z_Frame.csv"
        Z_frame.to_csv(z_name)

    fc = grinch.flat_clustering(model.aux['threshold'])
    logger.info('run_on_batch - threshold %s, linkages: min %s, max %s, avg %s, std %s',
                model.aux['threshold'],
                np.min(grinch.all_thresholds()),
                np.max(grinch.all_thresholds()),
                np.mean(grinch.all_thresholds()),
                np.std(grinch.all_thresholds()))
    tree_id = len(trees)
    trees.append(grinch)
    pids_list.append(all_pids)
    canopy_list.append(all_canopies)
    for i in range(len(all_pids)):
        if all_canopies[i] not in canopy2predictions:
            canopy2predictions[all_canopies[i]] = [[], []]
            canopy2tree[all_canopies[i]] = tree_id
        canopy2predictions[all_canopies[i]][0].append(all_pids[i])
        canopy2predictions[all_canopies[i]][1].append('%s-%s' % (all_canopies[i], fc[i]))
    return canopy2predictions


def needs_predicting(canopy_list, results, loader):
    return canopy_list
    return [c for c in canopy_list if c not in results]


def batcher(canopy_list, loader, min_batch_size=800):
    from pv.disambiguation.core import AssigneeNameMention
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

        records: typing.List[AssigneeNameMention] = loader.load(c)
        for record in records:
            pid = record.uuid
            if pid not in all_pids:
                all_pids.append(pid)
                all_lbls.append(-1)
                all_canopies.append(c)
                all_records.append(record)
        # lbls = -1 * np.ones(len(records))
        # all_canopies.extend([c for _ in range(len(pids))])
        # all_pids.extend(pids)
        # all_lbls.extend(lbls)
        # all_records.extend(records)
    if len(all_pids) > 0:
        yield list(all_pids), list(all_lbls), all_records, all_canopies


def run_batch(config, canopy_list, outdir, loader, chunk_id, job_name='disambig'):
    logger.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    canopy2tree_id = dict()
    tree_list = []
    pids_list = []
    pids_canopy_list = []
    outfile = os.path.join(outdir, job_name) + '.pkl'
    outstatefile = os.path.join(outdir, job_name) + 'internals.pkl'
    num_mentions_processed = 0
    num_canopies_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    to_run_on = needs_predicting(canopy_list, results, None)
    logger.info('had results for %s, running on %s', len(canopy_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logger.info('already had all canopies completed! wrapping up here...')
    encoding_model = AssigneeModel.from_config(config)
    weight_model = LinearAndRuleModel.from_encoding_model(encoding_model)
    weight_model.aux['threshold'] = 1.0 / (1.0 + float(config['assignee']['sim_threshold']))
    print(" -------------  -------------  -------------  ------------- ")
    print(f"USING float({config['assignee']['sim_threshold']}) THRESHOLD")
    print(" -------------  -------------  -------------  ------------- ")
    logger.info('[%s] using threshold %s ', job_name, weight_model.aux['threshold'])
    if to_run_on:
        for idx, (all_pids, all_lbls, all_records, all_canopies) in enumerate(
                batcher(to_run_on, loader, int(config['assignee']['min_batch_size']))):
            logger.info('[%s] run_batch %s - %s - processed %s mentions', job_name, idx, len(canopy_list),
                        num_mentions_processed)
            num_mentions_processed += len(all_pids)
            num_canopies_processed += np.unique(all_canopies).shape[0]
            run_on_batch(all_pids=all_pids, all_lbls=all_lbls, all_records=all_records, all_canopies=all_canopies,
                         model=weight_model, encoding_model=encoding_model, canopy2predictions=results,
                         canopy2tree=canopy2tree_id, trees=tree_list, pids_list=pids_list, canopy_list=pids_canopy_list,
                         job_name=job_name, batch=idx)
            if idx % 10 == 0:
                logger.info(
                    {'computed': idx + int(chunk_id) * int(config['assignee']['chunk_size']),
                     'num_mentions': num_mentions_processed})
            #     logger.info('[%s] caching results for job', job_name)
            #     with open(outfile, 'wb') as fin:
            #         pickle.dump(results, fin)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)

    logger.info('Beginning to save all tree structures....')
    grinch_trees = []
    for idx, t in tqdm(enumerate(tree_list), total=len(tree_list)):
        grinch = WeightedMultiFeatureGrinch.from_agglom(t, pids_list[idx], pids_canopy_list[idx])
        grinch.clear_node_features()
        grinch.points_set = False
        grinch_trees.append(grinch)
    torch.save([grinch_trees, canopy2tree_id], outstatefile)


def handle_singletons(canopy2predictions, singleton_canopies, loader):
    for s in singleton_canopies:
        ments = loader.load(s)
        assert len(ments) == 1
        canopy2predictions[s] = [[ments[0].uuid], [ments[0].uuid]]
    return canopy2predictions


def run_singletons(canopy_list, outdir, loader, job_name='disambig'):
    logger.info('need to run on %s canopies = %s ...', len(canopy_list), str(canopy_list[:5]))

    os.makedirs(outdir, exist_ok=True)
    results = dict()
    outfile = os.path.join(outdir, job_name) + '.pkl'
    num_mentions_processed = 0
    if os.path.exists(outfile):
        with open(outfile, 'rb') as fin:
            results = pickle.load(fin)

    to_run_on = needs_predicting(canopy_list, results, loader)
    logger.info('had results for %s, running on %s', len(canopy_list) - len(to_run_on), len(to_run_on))

    if len(to_run_on) == 0:
        logger.info('already had all canopies completed! wrapping up here...')

    if to_run_on:
        handle_singletons(results, to_run_on, loader)

    with open(outfile, 'wb') as fin:
        pickle.dump(results, fin)


def run_clustering(config):
    loader = Loader.from_config(config)
    all_canopies = set(loader.assignee_canopies.keys())
    # all_canopies = set([x for x in all_canopies])
    all_canopies = set([x for x in all_canopies if loader.num_records(x) < int(config['assignee']['max_canopy_size'])])
    singletons = set([x for x in all_canopies if loader.num_records(x) == 1])
    all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                                 reverse=True)
    logger.info('Number of canopies %s ', len(all_canopies_sorted))
    logger.info('Number of singletons %s ', len(singletons))
    logger.info('Largest canopies - ')
    for c in all_canopies_sorted[:10]:
        logger.info('%s - %s records', c, loader.num_records(c))
    outdir = config['assignee']['clustering_output_folder']
    num_chunks = int(len(all_canopies_sorted) // int(config['assignee']['chunk_size'])) + 1
    logger.info('%s num_chunks', num_chunks)
    logger.info('%s chunk_size', int(config['assignee']['chunk_size']))
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)

    for x in range(0, num_chunks):
        run_batch(config, chunks[x], outdir, loader, x, 'job-%s' % x)

    logger.info('Running singletons!!')
    run_singletons(list(singletons), outdir, job_name='job-singletons', loader=loader)
    with open(outdir + '/chunk2canopies.pkl', 'wb') as fout:
        pickle.dump([chunks, list(singletons)], fout)


def main(argv):
    logger.info('Running clustering - %s ', str(argv))

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/assignee/run_clustering.ini',
                 'config/database_tables.ini'])

    # if argv[] is a chunk id, then use this chunkid instead
    if len(argv) > 1:
        logger.info('Using cmd line arg for chunk id %s' % argv[1])
        config['assignee']['chunk_id'] = argv[1]
    run_clustering(config)
    # wandb.init(project="%s-%s" % (config['assignee']['exp_name'], config['assignee']['dataset_name']))
    # wandb.config.update(config)


if __name__ == "__main__":
    app.run(main)
