import collections
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from pathos.multiprocessing import ProcessingPool

from pv.disambiguation.core import InventorMention
import pv.disambiguation.util.db as pvdb
import configparser

#
# FLAGS = flags.FLAGS
# flags.DEFINE_string('feature_out', 'data/inventor/coinventor_features', '')

import os


def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants(config):
    cnx = pvdb.pregranted_table(config)
    cursor = cnx.cursor()
    query = "SELECT id, document_number, name_first, name_last FROM rawinventor;"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for uuid, document_number, name_first, name_last in cursor:
        im = InventorMention(uuid, None, '', name_first if name_first else '', name_last if name_last else '', '', '',
                             '', document_number=document_number)
        feature_map[im.record_id].append(last_name(im))
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def build_granted(config):
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, patent_id, name_first, name_last FROM rawinventor;"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for uuid, patent_id, name_first, name_last in cursor:
        im = InventorMention(uuid, patent_id, '', name_first if name_first else '', name_last if name_last else '', '',
                             '', '')
        feature_map[im.record_id].append(last_name(im))
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s granted records - %s features', 10000, idx, len(feature_map))
    return feature_map


def run(source):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_coinventor_features_sql.ini'])
    if source == 'pregranted':
        features = build_pregrants(config)
    elif source == 'granted':
        features = build_granted(config)
    return features


def main(argv):
    logging.info('Building coinventor features')
    feats = [n for n in ProcessingPool().imap(run, ['granted', 'pregranted'])]
    features = feats[0]
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_coinventor_features_sql.ini'])
    for i in range(1, len(feats)):
        features.update(feats[i])
    with open(config['INVENTOR_BUILD_COINVENTOR_FEAT']['feature_out'] + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


if __name__ == "__main__":
    app.run(main)
