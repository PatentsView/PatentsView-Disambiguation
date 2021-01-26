import collections
import os
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

import pv.disambiguation.util.db as pvdb
import configparser

def load_disambiguation(granted_uuid, pregranted_uuid, config):
    logging.info('disambiguation loading....')
    uuid2entityid = dict()
    with open(config['ASSIGNEE_LOCATION_CANOPIES']['disambiguation'], 'r') as fin:
        for line in tqdm(fin, desc='load disambiguation', total=9560321):
            splt = line.strip().split('\t')
            if splt[0] in pregranted_uuid:
                uuid2entityid[pregranted_uuid[splt[0]]] = splt[1]
            elif splt[0] in granted_uuid:
                uuid2entityid[granted_uuid[splt[0]]] = splt[1]
    return uuid2entityid


def build_granted(granted_uuids, pgranted_uuids, config):
    canopy2uuids = collections.defaultdict(list)
    uuid2canopy = dict()
    uuid2entityid = load_disambiguation(granted_uuids, pgranted_uuids, config)
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, rawlocation_id FROM rawassignee;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=6789244):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
        uuid2canopy[rawlocation_id] = uuid2entityid[uuid]
    return canopy2uuids, uuid2canopy


def build_pregrants(granted_uuids, pgranted_uuids, config):
    canopy2uuids = collections.defaultdict(list)
    uuid2canopy = dict()
    uuid2entityid = load_disambiguation(granted_uuids, pgranted_uuids, config)
    cnx = pvdb.pregranted_table(config)
    cursor = cnx.cursor()
    query = "SELECT id, rawlocation_id FROM rawassignee;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=2771077):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
        uuid2canopy[rawlocation_id] = uuid2entityid[uuid]
    return canopy2uuids, uuid2canopy


def collection_location_mentions_granted(granted_uuids, pgranted_uuids, config):
    canopy2uuids = collections.defaultdict(list)
    uuid2entityid = load_disambiguation(granted_uuids, pgranted_uuids, config)
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, rawlocation_id FROM rawassignee;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=18000000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
    return canopy2uuids


def main(argv):
    logging.info('Building canopies')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/location/build_assignee_location_canopies.ini'])

    granted_uuids, pgranted_uuids = pickle.load(open(config['ASSIGNEE_LOCATION_CANOPIES']['uuidmap'], 'rb'))
    source = 'pregranted'
    canopies, uuid2canopy = build_pregrants(granted_uuids, pgranted_uuids, config)
    with open(config['ASSIGNEE_LOCATION_CANOPIES']['canopy_out'] + '.%s.pkl' % source, 'wb') as fout:
        pickle.dump([canopies, uuid2canopy], fout)

    source = 'granted'
    canopies, uuid2canopy = build_granted(granted_uuids, pgranted_uuids, config)
    with open(config['ASSIGNEE_LOCATION_CANOPIES']['canopy_out'] + '.%s.pkl' % source, 'wb') as fout:
        pickle.dump([canopies, uuid2canopy], fout)


if __name__ == "__main__":
    app.run(main)
