import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from pathos.multiprocessing import ProcessingPool

import os
import pv.disambiguation.util.db as pvdb
import configparser

def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants(config):
    feature_map = dict()
    cnx = pvdb.incremental_pregranted_table(config)
    if cnx is None:
        return feature_map
    cursor = cnx.cursor()
    query = "select document_number,invention_title from application;"
    cursor.execute(query)
    idx = 0
    for rec in cursor:
        record_id = 'pg-%s' % rec[0]
        feature_map[record_id] = rec[1]
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    logging.log(logging.INFO, 'Processed %s pregrant records - %s features', idx, len(feature_map))
    return feature_map


def build_granted(config):
    feature_map = dict()
    cnx = pvdb.incremental_granted_table(config)
    if cnx is None:
        return feature_map
    cursor = cnx.cursor()
    query = "SELECT id,title FROM patent;"
    cursor.execute(query)
    idx = 0
    for rec in cursor:
        record_id = '%s' % rec[0]
        feature_map[record_id] = rec[1]
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s grant records - %s features', 10000, idx, len(feature_map))
    logging.log(logging.INFO, 'Processed %s grant records - %s features', idx, len(feature_map))
    return feature_map


def run(source):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_title_map_sql.ini'])
    if source == 'pregranted':
        features = build_pregrants(config)
    elif source == 'granted':
        features = build_granted(config)
    return features


def main(argv):
    logging.info('Building title features')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_title_map_sql.ini'])

    # create output folder if it doesn't exist
    logging.info('writing results to folder: %s',
                 os.path.dirname(config['INVENTOR_BUILD_TITLES']['feature_out']))
    os.makedirs(os.path.dirname(config['INVENTOR_BUILD_TITLES']['feature_out']), exist_ok=True)

    feats = [n for n in ProcessingPool().imap(run, ['granted', 'pregranted'])]

    with open(config['INVENTOR_BUILD_TITLES']['base_title_map'], 'rb') as fin:
        features = pickle.load(fin)

    for i in range(0, len(feats)):
        features.update(feats[i])

    with open(config['INVENTOR_BUILD_TITLES']['feature_out'] + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


if __name__ == "__main__":
    app.run(main)
