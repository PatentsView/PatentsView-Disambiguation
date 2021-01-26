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

def main(argv):
    logging.info('Running clustering - %s ', str(argv))

    # Load the config files
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/inventor/run_clustering.ini', 'config/database_tables.ini'])
    logging.info('Config - %s', str(config))

    # A connection to the SQL database that will be used to load the inventor data.
    loader = Loader.from_config(config, 'inventor')

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
    outdir = os.path.join(config['inventor']['outprefix'], 'inventor', config['inventor']['run_id'])

    # the number of chunks based on the specified chunksize
    num_chunks = int(len(all_canopies_sorted) / int(config['inventor']['chunk_size']))

    logging.info('%s num_chunks', num_chunks)
    logging.info('%s chunk_size', int(config['inventor']['chunk_size']))
    logging.info('%s chunk_id', int(config['inventor']['chunk_id']))

    # chunk all of the data by canopy
    chunks = [[] for _ in range(num_chunks)]
    for idx, c in enumerate(all_canopies_sorted):
        chunks[idx % num_chunks].append(c)

    this_chunk = chunks[int(config['inventor']['chunk_id'])]
    this_pre = dict()
    this_g = dict()
    for c in this_chunk:
        if c in loader.pregranted_canopies:
            this_pre[c] = [loader.pregranted_canopies[c][0]]
        if c in loader.granted_canopies:
            this_g[c] = [loader.granted_canopies[c][0]]

    with open('data/inventor/tmp.test.pre_granted.canopies.pkl', 'wb') as fout:
        pickle.dump(this_pre, fout)
    with open('data/inventor/tmp.test.granted.canopies.pkl', 'wb') as fout:
        pickle.dump(this_g, fout)

if __name__ == "__main__":
    app.run(main)
