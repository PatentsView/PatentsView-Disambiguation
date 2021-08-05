import collections
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
import configparser

from pv.disambiguation.core import AssigneeMention, AssigneeNameMention
import pv.disambiguation.util.db as pvdb

import os


def last_name(im):
    return im.last_name()[0] if len(im.last_name()) > 0 else im.uuid


def build_pregrants(config):
    # | id | document_number | sequence | name_first | name_last | organization | type | rawlocation_id | city | state | country | filename | created_date | updated_date |
    cnx = pvdb.pregranted_table(config)
    cursor = cnx.cursor()
    query = "SELECT id, document_number, sequence -1 as sequence, name_first, name_last, organization, type, rawlocation_id, city, state, country FROM rawassignee"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_application_sql_record(rec)
        feature_map[am.name_features()[0]].append(am)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def build_granted(config):
    # | uuid | patent_id | assignee_id | rawlocation_id | type | name_first | name_last | organization | sequence |
    cnx = pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid , patent_id , assignee_id , rawlocation_id , type , name_first , name_last , organization , sequence FROM rawassignee;"
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_granted_sql_record(rec)
        feature_map[am.name_features()[0]].append(am)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s granted records - %s features', 10000, idx, len(feature_map))
    return feature_map


def run(args):
    source, config = args[0], args[1]
    if source == 'pregranted':
        features = build_pregrants(config)
    elif source == 'granted':
        features = build_granted(config)
    return features


def main(argv):
    logging.info('Building assignee mentions')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/build_name_mentions_sql.ini'])

    feats = [n for n in map(run, [('granted',config), ('pregranted',config)])]
    # feats = [run('granted')]
    logging.info('finished loading mentions %s', len(feats))
    name_mentions = set(feats[0].keys()).union(set(feats[1].keys()))
    logging.info('number of name mentions %s', len(name_mentions))
    from tqdm import tqdm
    records = dict()
    from collections import defaultdict
    canopies = defaultdict(set)
    for nm in tqdm(name_mentions, 'name_mentions'):
        anm = AssigneeNameMention.from_assignee_mentions(nm, feats[0][nm] + feats[1][nm])
        for c in anm.canopies:
            canopies[c].add(anm.uuid)
        records[anm.uuid] = anm

    with open(config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out'] + '.%s.pkl' % 'records', 'wb') as fout:
        pickle.dump(records, fout)
    with open(config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out'] + '.%s.pkl' % 'canopies', 'wb') as fout:
        pickle.dump(canopies, fout)


if __name__ == "__main__":
    app.run(main)
