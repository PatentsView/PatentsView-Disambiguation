import collections
import configparser
import pickle

from absl import app
from absl import logging
from pathos.multiprocessing import ProcessingPool

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import AssigneeMention
import os

def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants(config):
    # | id | document_number | sequence | name_first | name_last | organization | type | rawlocation_id | city | state | country | filename | created_date | updated_date |
    feature_map = collections.defaultdict(list)
    cnx = pvdb.pregranted_table(config)
    # if there was no table specified
    if cnx is None:
        return feature_map
    cursor = cnx.cursor()
    query = "SELECT id, document_number, sequence-1 as sequence, name_first, name_last, organization, type, rawlocation_id, city, state, country FROM rawassignee"
    cursor.execute(query)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_application_sql_record(rec)
        feature_map[am.record_id].append(am.assignee_name())
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    logging.log(logging.INFO, 'Processed %s pregrant records - %s features', idx, len(feature_map))
    return feature_map


def build_granted(config):
    # | uuid | patent_id | assignee_id | rawlocation_id | type | name_first | name_last | organization | sequence |
    feature_map = collections.defaultdict(list)
    cnx = pvdb.granted_table(config)
    # if there was no table specified
    if cnx is None:
        return feature_map
    cursor = cnx.cursor()
    query = "SELECT uuid, patent_id, assignee_id, rawlocation_id, type, name_first, name_last, organization, sequence FROM rawassignee"
    cursor.execute(query)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_granted_sql_record(rec)
        feature_map[am.record_id].append(am.assignee_name())
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s granted records - %s features', 10000, idx, len(feature_map))
    logging.log(logging.INFO, 'Processed %s granted records - %s features', idx, len(feature_map))
    return feature_map


def run(source):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_assignee_features_sql.ini'])
    if source == 'pregranted':
        features = build_pregrants(config)
    elif source == 'granted':
        features = build_granted(config)
    return features


def main(argv):
    logging.info('Building assignee features')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_assignee_features_sql.ini'])

    # create output folder if it doesn't exist
    logging.info('writing results to folder: %s', os.path.dirname(config['INVENTOR_BUILD_ASSIGNEE_FEAT']['feature_out']))
    os.makedirs(os.path.dirname(config['INVENTOR_BUILD_ASSIGNEE_FEAT']['feature_out']), exist_ok=True)

    feats = [n for n in ProcessingPool().imap(run, ['granted', 'pregranted'])]
    features = feats[0]
    for i in range(1, len(feats)):
        features.update(feats[i])
    with open(config['INVENTOR_BUILD_ASSIGNEE_FEAT']['feature_out'] + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


if __name__ == "__main__":
    app.run(main)
