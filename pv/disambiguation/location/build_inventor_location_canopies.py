import collections
import os
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm
import configparser

# FLAGS = flags.FLAGS
# flags.DEFINE_string('canopy_out', 'data/location/canopies.inventor', '')
# flags.DEFINE_string('source', 'pregranted', 'pregranted or granted')
# flags.DEFINE_string('disambiguation', 'exp_out/inventor/run_23/disambiguation.tsv', '')
import pv.disambiguation.util.db as pvdb


def load_disambiguation(config):
    logging.info('disambiguation loading....')
    uuid2entityid = dict()
    with open(config['INVENTOR_LOCATION_CANOPIES']['disambiguation'], 'r') as fin:
        for line in tqdm(fin, desc='load disambiguation', total=18000000):
            splt = line.strip().split('\t')
            if len(splt) == 2:
                uuid2entityid[splt[0]] = splt[1]
            else:
                logging.error('malformed line %s', line)
    return uuid2entityid


def build_granted(config):
    canopy2uuids = collections.defaultdict(list)
    uuid2canopy = dict()
    uuid2entityid = load_disambiguation(config)
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, rawlocation_id FROM rawinventor;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=18000000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
        uuid2canopy[rawlocation_id] = uuid2entityid[uuid]
    return canopy2uuids, uuid2canopy


def build_pregrants(config):
    canopy2uuids = collections.defaultdict(list)
    uuid2canopy = dict()
    uuid2entityid = load_disambiguation(config)
    cnx = pvdb.pregranted_table(config)
    cursor = cnx.cursor()
    query = "SELECT id, rawlocation_id FROM rawinventor;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=8100000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
        uuid2canopy[rawlocation_id] = uuid2entityid[uuid]
    return canopy2uuids, uuid2canopy


def collection_location_mentions_granted(config):
    canopy2uuids = collections.defaultdict(list)
    uuid2entityid = load_disambiguation(config)
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, rawlocation_id FROM rawinventor;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=18000000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
    return canopy2uuids


def main(argv):
    logging.info('Building canopies')
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/location/build_inventor_location_canopies.ini'])

    source = 'pregranted'
    canopies, uuid2canopy = build_pregrants(config)
    with open(config['INVENTOR_LOCATION_CANOPIES']['canopy_out'] + '.%s.pkl' % source, 'wb') as fout:
        pickle.dump([canopies, uuid2canopy], fout)

    source = 'granted'
    canopies, uuid2canopy = build_granted(config)
    with open(config['INVENTOR_LOCATION_CANOPIES']['canopy_out'] + '.%s.pkl' % source, 'wb') as fout:
        pickle.dump([canopies, uuid2canopy], fout)


if __name__ == "__main__":
    app.run(main)
