import collections
import configparser
import pickle

from absl import app
from absl import logging
from pathos.multiprocessing import ProcessingPool

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import AssigneeMention


def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants(config):
    # | id | document_number | sequence | name_first | name_last | organization | type | rawlocation_id | city | state | country | filename | created_date | updated_date |
    cnx = pvdb.pregranted_table(config)
    cursor = cnx.cursor()
    query = "SELECT id, document_number, sequence-1 as sequence, name_first, name_last, organization, type, rawlocation_id, city, state, country FROM rawassignee"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_application_sql_record(rec)
        feature_map[am.record_id].append(am.assignee_name())
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def build_granted(config):
    # | uuid | patent_id | assignee_id | rawlocation_id | type | name_first | name_last | organization | sequence |
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, patent_id, assignee_id, rawlocation_id, type, name_first, name_last, organization, sequence FROM rawassignee"
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
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_assignee_features_sql.ini'])
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
                 'config/inventor/build_assignee_features_sql.ini'])
    for i in range(1, len(feats)):
        features.update(feats[i])
    with open(config['INVENTOR_BUILD_ASSIGNEE_FEAT']['feature_out'] + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


if __name__ == "__main__":
    app.run(main)
