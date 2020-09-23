import collections
import os
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

from pv.disambiguation.core import LocationMention, LocationNameMention

FLAGS = flags.FLAGS
flags.DEFINE_string('canopy', 'data/location/canopies', '')
flags.DEFINE_string('output', 'data/location/inventor_location.mentions.pkl', '')


def build_granted(granted_uuid2canopy):
    canopy2mentions = collections.defaultdict(list)
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='patent_20200630')
    cursor = cnx.cursor()
    query = "SELECT * FROM rawlocation;"
    cursor.execute(query)
    for rec in tqdm(cursor, 'process', total=18000000):
        lm = LocationMention.from_granted_sql_record(rec)
        if lm.uuid in granted_uuid2canopy:
            canopy = granted_uuid2canopy[lm.uuid]
            canopy2mentions[canopy].append(lm)
    return canopy2mentions


def build_pregrants(canopy2mentions, pregranted_uuid2canopy):
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='pregrant_publications')
    cursor = cnx.cursor()
    query = "SELECT * FROM rawlocation;"
    cursor.execute(query)
    for rec in tqdm(cursor, 'process', total=18000000):
        lm = LocationMention.from_granted_sql_record(rec)
        if lm.uuid in pregranted_uuid2canopy:
            canopy = pregranted_uuid2canopy[lm.uuid]
            canopy2mentions[canopy].append(lm)
    return canopy2mentions


def build_name_mentions(canopy2mentions):
    canopy2name_mentions = collections.defaultdict(list)
    for c, m in tqdm(canopy2mentions.items()):
        res = collections.defaultdict(list)
        for ment in m:
            res[ment.canonical_string()].append(ment)
        for n, ments in res.items():
            canopy2name_mentions[c].append(LocationNameMention.from_mentions(ments))
    return canopy2name_mentions


def main(argv):
    logging.info('Building mentions')
    logging.info('loading canopies [pregranted] ... ')

    with open(FLAGS.canopy + '.pregranted.pkl', 'rb') as fin:
        pregranted_canopies, pregranted_uuid2canopy = pickle.load(fin)
    logging.info('loading canopies [pregranted] ... done.')

    logging.info('loading canopies [granted] ... ')
    with open(FLAGS.canopy + '.granted.pkl', 'rb') as fin:
        granted_canopies, granted_uuid2canopy = pickle.load(fin)
    logging.info('loading canopies [granted] ... done ')

    mentions = build_granted(granted_uuid2canopy)
    logging.info('len(mentions) = %s ', len(mentions))
    mentions = build_pregrants(mentions, pregranted_uuid2canopy)
    logging.info('len(mentions) = %s ', len(mentions))
    canopy2name_mentions = build_name_mentions(mentions)
    with open(FLAGS.output, 'wb') as fout:
        pickle.dump(canopy2name_mentions, fout)


if __name__ == "__main__":
    app.run(main)
