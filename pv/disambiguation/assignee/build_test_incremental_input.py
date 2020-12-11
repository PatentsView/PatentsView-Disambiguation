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

logging.set_verbosity(logging.INFO)

def main(argv):
    logging.info('Running clustering - %s ', str(argv))

    # Load the config files
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/assignee/run_clustering.ini', 'config/database_tables.ini'])
    logging.info('Config - %s', str(config))

    # A connection to the SQL database that will be used to load the assignee data.
    loader = Loader.from_config(config)

    # Find all of the canopies in the entire dataset.
    all_canopies = set(loader.assignee_canopies.keys())
    all_canopies = set([x for x in all_canopies if loader.num_records(x) < int(config['assignee']['max_canopy_size'])])
    singletons = set([x for x in all_canopies if loader.num_records(x) == 1])
    all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                                 reverse=True)
    num_chunks = int(len(all_canopies_sorted) / int(config['assignee']['chunk_size']))

    logging.info('%s num_chunks', num_chunks)
    logging.info('%s chunk_size', int(config['assignee']['chunk_size']))
    logging.info('%s chunk_id', int(config['assignee']['chunk_id']))

    # chunk all of the data by canopy
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)

    # Find some stats of the data before chunking it
    logging.info('Number of canopies %s ', len(all_canopies))

    canopy_sample = chunks[config['assignee']['chunk_id']]
    mention_sample_canopies = dict()
    mention_sample_records = dict()

    for c in canopy_sample:
        mention_sample_canopies[c] = loader.assignee_canopies[c][:5]
        mention_sample_records[c] = [loader.assignee_mentions[m] for m in mention_sample_canopies[c]]

    with open('data/assignee/tmp.test.assignee_canopies.pkl', 'wb') as fout:
        pickle.dump(mention_sample_canopies, fout)
    with open('data/assignee/tmp.test.assignee_mentions.pkl', 'wb') as fout:
        pickle.dump(mention_sample_records, fout)

if __name__ == "__main__":
    app.run(main)
