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

from pv.disambiguation.assignee.load_name_mentions import Loader
from pv.disambiguation.assignee.model import AssigneeModel
from grinch.model import LinearAndRuleModel

logging.set_verbosity(logging.INFO)


def run(config, loader, new_canopies, chunks, singleton_list,
              outdir, job_name='disambig'):
    logging.info('need to run on %s canopies = %s ...', len(new_canopies), str(new_canopies[:5]))

    os.makedirs(outdir, exist_ok=True)

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

    encoding_model = AssigneeModel.from_config(config)
    weight_model = LinearAndRuleModel.from_encoding_model(encoding_model)
    weight_model.aux['threshold'] = config['assignee']['sim_threshold']

    for c in new_canopies:
        if c in canopy2chunks:
            new_canopies_by_chunk[canopy2chunks[c]].append(c)
        else:
            new_canopies_by_chunk[next_chunk].append(c)

    for this_chunk_id, this_chunk_canopies in new_canopies_by_chunk.items():
        if this_chunk_id != next_chunk:

            # Load the grinch trees
            statefile = os.path.join(outdir, 'job-%s' % this_chunk_id) + 'internals.pkl'
            [grinch_trees, canopy2tree_id] = torch.load(statefile)

            # load the old predictions
            predfile = os.path.join(outdir, 'job-%s' % this_chunk_id) + '.pkl'
            with open(predfile, 'rb') as fin:
                canopy2predictions = pickle.load(fin)

            canopies_per_tree = collections.defaultdict(list)
            for c in this_chunk_canopies:
                canopies_per_tree[canopy2tree_id[c]].append(c)

            for tree_id,canopy_list in canopies_per_tree.items():

                all_pids = []
                all_lbls = []
                all_records = []
                all_canopies = []
                for c in canopy_list:
                    records = loader.load(c)
                    pids = [x.uuid for x in records]
                    lbls = -1 * np.ones(len(records))
                    all_canopies.extend([c for _ in range(len(pids))])
                    all_pids.extend(pids)
                    all_lbls.extend(lbls)
                    all_records.extend(records)

                features = encoding_model.encode(all_records)
                grinch = grinch_trees[tree_id]
                grinch.update_and_insert(features, all_pids)
                grinch.clear_node_features()
                grinch.points_set = False
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



def main(argv):
    logging.info('Running clustering - %s ', str(argv))

    # Load the config files
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini',
                 'config/assignee/run_incremental.ini',
                 'config/database_tables.ini'])
    logging.info('Config - %s', str(config))

    # A connection to the SQL database that will be used to load the inventor data.
    # Note that this will specifically load the
    # new canopies from config[config_type]['pregranted_canopies']
    # and config[config_type]['granted_canopies']
    loader = Loader.from_config(config)

    # note that this is a map from canopy_name to mention_id
    new_canopies = set(loader.pregranted_canopies.keys()).union(set(loader.granted_canopies.keys()))

    # setup the output dir
    outdir = os.path.join(config['assignee']['outprefix'], 'assignee', config['assignee']['run_id'])

    # determine where the canopies have been stored in chunks and singletons.
    with open(outdir + '/chunk2canopies.pkl', 'rb') as fout:
        [chunks, singletons] = pickle.load(fout)

    logging.info('Number of new canopies %s ', len(new_canopies))

    # run the job for the given batch
    run(config, loader, list(new_canopies), chunks, list(singletons), outdir,
              job_name='job-incremental')



if __name__ == "__main__":
    app.run(main)
