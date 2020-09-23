import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from pathos.multiprocessing import ProcessingPool

FLAGS = flags.FLAGS
flags.DEFINE_string('feature_out', 'data/inventor/title_features', '')

import os


def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants():
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='pregrant_publications')
    cursor = cnx.cursor()
    query = "select document_number,invention_title from application;"
    cursor.execute(query)
    feature_map = dict()
    idx = 0
    for rec in cursor:
        record_id = 'pg-%s' % rec[0]
        feature_map[record_id] = rec[1]
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def build_granted():
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='patent_20200630')
    cursor = cnx.cursor()
    query = "SELECT id,title FROM patent;"
    cursor.execute(query)
    feature_map = dict()
    idx = 0
    for rec in cursor:
        record_id = '%s' % rec[0]
        feature_map[record_id] = rec[1]
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s grant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def run(source):
    if source == 'pregranted':
        features = build_pregrants()
    elif source == 'granted':
        features = build_granted()
    return features


def main(argv):
    logging.info('Building coinventor features')
    feats = [n for n in ProcessingPool().imap(run, ['granted', 'pregranted'])]
    features = feats[0]
    for i in range(1, len(feats)):
        features.update(feats[i])
    with open(FLAGS.feature_out + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


if __name__ == "__main__":
    app.run(main)
