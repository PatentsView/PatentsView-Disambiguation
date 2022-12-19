import datetime

import collections
import configparser
from os.path import basename
from typing import Optional, Any
import os
import billiard as mp

from pathos.multiprocessing import ProcessingPool

from absl import logging, app
import pickle

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import AssigneeMention


def build_assignee_mentions_for_type(config, source='granted_patent_database'):
    feature_map = collections.defaultdict(list)
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    # if there was no table specified
    if cnx is None:
        return feature_map
    where_clause = ''
    if config['DISAMBIGUATION']['INCREMENTAL'] == "1":
        where_clause = "where ra.version_indicator between '{start_date}' and '{end_date}'".format(
            start_date=config['DATES']['START_DATE'], end_date=config['DATES']['END_DATE'])
        # | id | document_number | sequence | name_first | name_last | organization | type |
        # rawlocation_id | city | state | country | filename | created_date | updated_date |
    id_field = "id"
    sequence_field = "sequence-1"
    document_id_field = "document_number"
    if source == 'granted_patent_database':
        id_field = "uuid"
        sequence_field = "sequence"
        document_id_field = 'patent_id'
    query = """
        SELECT ra.{id_field}, ra.{document_id_field},  ra.{sequence_field} as sequence, ra.name_first,
         ra.name_last, ra.organization, ra.type, ra.rawlocation_id, rl.city, rl.state, rl.country
        FROM rawassignee ra left join rawlocation rl on rl.id = ra.rawlocation_id {where_clause}
    """.format(where_clause=where_clause, id_field=id_field, sequence_field=sequence_field,
               document_id_field=document_id_field)
    cursor = cnx.cursor(dictionary=True)
    cursor.execute(query)
    idx = 0
    rec: dict
    for rec in cursor:
        am = AssigneeMention.from_sql_records(rec)
        feature_map[am.record_id].append(am.assignee_name())
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s %s records - %s features', 10000, source, idx,
                            len(feature_map))
    logging.log(logging.INFO, 'Processed %s %s  records - %s features', idx, source, len(feature_map))
    return feature_map


def generate_assignee_mentions(config):
    logging.info('Building assignee features')
    features = collections.defaultdict(list)
    end_date = config["DATES"]["END_DATE"]
    # If running incremental disambiguation
    if config['DISAMBIGUATION']['INCREMENTAL'] == "1":
        # Load latest full disambiguation results
        with open(config['INVENTOR_BUILD_ASSIGNEE_FEAT']['base_assignee_features'], 'rb') as fin:
            features = pickle.load(fin)
    # Generate mentions from granted and pregrant databases
    pool = mp.Pool()
    feats = [
        n for n in pool.starmap(
            build_assignee_mentions_for_type, [
                (config, 'granted_patent_database'),
                (config, 'pregrant_database')
            ])
    ]
    for i in range(0, len(feats)):
        features.update(feats[i])
    # create output folder if it doesn't exist
    path = f"{config['BASE_PATH']['assignee']}".format(end_date=end_date) + config['INVENTOR_BUILD_ASSIGNEE_FEAT']['feature_out']
    logging.info('writing results to folder: %s', os.path.dirname(path))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Serialize generated mentions
    with open(path + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_assignee_features_sql.ini'])
    generate_assignee_mentions(config)


if __name__ == "__main__":
    app.run(main)
