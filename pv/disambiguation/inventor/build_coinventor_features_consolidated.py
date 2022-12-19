import collections
import configparser
import billiard as mp
import os
import pickle
import datetime

from absl import logging, app

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import InventorMention
from pv.disambiguation.util.config_util import generate_incremental_components
from pv.disambiguation.util.text_utils import last_name
from lib.configuration import get_disambig_config


def build_coinventor_mentions_for_type(config, source='granted_patent_database'):
    feature_map = collections.defaultdict(list)
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    # if there was no table specified
    if cnx is None:
        return feature_map
    ignore_filters = config['DISAMBIGUATION'].get('debug', 0)
    incremental_components = generate_incremental_components(config, source,
                                                             db_table_prefix='ri', ignore_filters=ignore_filters)
    # | id | document_number | sequence | name_first | name_last | organization | type |
    # rawlocation_id | city | state | country | filename | created_date | updated_date |
    query = """
        SELECT {id_field}, {document_id_field},   ri.name_first,
         ri.name_last
        FROM rawinventor ri {filter}
    """.format(filter=incremental_components.get('filter'),
               id_field=incremental_components.get('id_field'),
               document_id_field=incremental_components.get('document_id_field'))
    cursor = cnx.cursor(dictionary=True)
    cursor.execute(query)
    idx = 0
    rec: dict
    for rec in cursor:
        im = InventorMention.from_sql_record_dict(rec)
        feature_map[im.record_id].append(last_name(im))
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s %s records - %s features', 10000, source, idx,
                            len(feature_map))
    logging.log(logging.INFO, 'Processed %s %s  records - %s features', idx, source, len(feature_map))
    return feature_map


def generate_coinventor_mentions(config):
    logging.info('Building assignee features')
    features = collections.defaultdict(list)
    end_date = config["DATES"]["END_DATE"]
    # If running incremental disambiguation
    if config['DISAMBIGUATION']['INCREMENTAL'] == "1":
        # Load latest full disambiguation results
        with open(config['INVENTOR_BUILD_COINVENTOR_FEAT']['base_coinventor_features'], 'rb') as fin:
            features = pickle.load(fin)
    # Generate mentions from granted and pregrant databases
    pool = mp.Pool()
    feats = [
        n for n in pool.starmap(
            build_coinventor_mentions_for_type, [
                (config, 'granted_patent_database'),
                (config, 'pregrant_database')
            ])
    ]
    for i in range(0, len(feats)):
        features.update(feats[i])
    # create output folder if it doesn't exist
    logging.info('writing results to folder: %s', os.path.dirname(config['INVENTOR_BUILD_COINVENTOR_FEAT']['feature_out']))
    path = f"{config['BASE_PATH']['inventor']}".format(end_date=end_date) + config['INVENTOR_BUILD_COINVENTOR_FEAT']['feature_out']
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Serialize generated mentions
    with open(config['INVENTOR_BUILD_COINVENTOR_FEAT']['feature_out'] + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


def main(argv):
    # config.read(['config/database_config.ini', 'config/database_tables.ini',
    #              'config/inventor/build_coinventor_features_sql.ini'])
    # config.read(['config/database_config.ini', 'config/database_tables.ini', 'config/inventor/build_coinventor_features_sql.ini'])
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=[f'config/consolidated_config.ini'],
                                 **{
                                     "execution_date": datetime.date(2022, 7, 1)
                                 })
    generate_coinventor_mentions(config)


if __name__ == "__main__":
    app.run(main)
