import collections
import os
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

FLAGS = flags.FLAGS
flags.DEFINE_string('canopy_out', 'data/location/canopies.inventor', '')
flags.DEFINE_string('source', 'pregranted', 'pregranted or granted')
flags.DEFINE_string('disambiguation', 'exp_out/inventor/run_23/disambiguation.tsv', '')


def load_disambiguation():
    logging.info('disambiguation loading....')
    uuid2entityid = dict()
    with open(FLAGS.disambiguation, 'r') as fin:
        for line in tqdm(fin, desc='load disambiguation', total=18000000):
            splt = line.strip().split('\t')
            uuid2entityid[splt[0]] = splt[1]
    return uuid2entityid


def build_granted():
    canopy2uuids = collections.defaultdict(list)
    uuid2canopy = dict()
    uuid2entityid = load_disambiguation()
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='patent_20200630')
    cursor = cnx.cursor()
    query = "SELECT uuid, rawlocation_id FROM rawinventor;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=18000000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
        uuid2canopy[rawlocation_id] = uuid2entityid[uuid]
    return canopy2uuids, uuid2canopy


def build_pregrants():
    canopy2uuids = collections.defaultdict(list)
    uuid2canopy = dict()
    uuid2entityid = load_disambiguation()
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='pregrant_publications')
    cursor = cnx.cursor()
    query = "SELECT id, rawlocation_id FROM rawinventor;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=8100000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
        uuid2canopy[rawlocation_id] = uuid2entityid[uuid]
    return canopy2uuids, uuid2canopy


def collection_location_mentions_granted():
    canopy2uuids = collections.defaultdict(list)
    uuid2entityid = load_disambiguation()
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='pregrant_publications')
    cursor = cnx.cursor()
    query = "SELECT uuid, rawlocation_id FROM rawinventor;"
    cursor.execute(query)
    for uuid, rawlocation_id in tqdm(cursor, 'process', total=18000000):
        canopy2uuids[uuid2entityid[uuid]].append(rawlocation_id)
    return canopy2uuids


def main(argv):
    logging.info('Building canopies')
    if FLAGS.source == 'pregranted':
        canopies, uuid2canopy = build_pregrants()
    elif FLAGS.source == 'granted':
        canopies, uuid2canopy = build_granted()
    with open(FLAGS.canopy_out + '.%s.pkl' % FLAGS.source, 'wb') as fout:
        pickle.dump([canopies, uuid2canopy], fout)


if __name__ == "__main__":
    app.run(main)
