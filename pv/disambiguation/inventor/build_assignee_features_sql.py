import collections
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
from pathos.multiprocessing import ProcessingPool

from pv.disambiguation.core import AssigneeMention

FLAGS = flags.FLAGS
flags.DEFINE_string('feature_out', 'data/inventor/assignee_features', '')

import os


def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants():
    # | id | document_number | sequence | name_first | name_last | organization | type | rawlocation_id | city | state | country | filename | created_date | updated_date |
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='pregrant_publications')
    cursor = cnx.cursor()
    query = "SELECT * FROM rawassignee;"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_application_sql_record(rec)
        feature_map[am.record_id].append(am.assignee_name())
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def build_granted():
    # | uuid | patent_id | assignee_id | rawlocation_id | type | name_first | name_last | organization | sequence |
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='patent_20200630')
    cursor = cnx.cursor()
    query = "SELECT * FROM rawassignee;"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_granted_sql_record(rec)
        feature_map[am.record_id].append(am.assignee_name())
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s granted records - %s features', 10000, idx, len(feature_map))
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
