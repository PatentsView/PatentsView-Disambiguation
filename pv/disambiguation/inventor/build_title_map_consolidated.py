import configparser
import billiard as mp
import os
import pickle

from absl import app
from absl import logging

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.util.config_util import generate_incremental_components


def build_title_map_for_source(config, source='granted_patent_database'):
    feature_map = dict()
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    if cnx is None:
        return feature_map
    # id_field | document_id_field | central_entity_field | sequence_field | title_table | title_field | record_id_format
    ignore_filters = config['DISAMBIGUATION'].get('debug', 0)
    incremental_components = generate_incremental_components(config, source, 'a', ignore_filters)
    cursor = cnx.cursor(dictionary=True)
    query = "select {central_entity_field},{title_field} from {title_table} a {filter};".format(
        central_entity_field=incremental_components.get('central_entity_field'),
        title_field=incremental_components.get('title_field'),
        filter=incremental_components.get('filter'),
        title_table=incremental_components.get('title_table'))
    logging.log(logging.INFO, query)
    cursor.execute(query)
    idx = 0
    for rec in cursor:
        record_id = incremental_components.get('record_id_format') % rec.get(
            incremental_components.get('central_entity_field'))
        feature_map[record_id] = rec.get(incremental_components.get('title_field'))
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s %s records - %s features', 10000, source, idx, len(feature_map))
    logging.log(logging.INFO, 'Processed %s %s records - %s features', idx, source, len(feature_map))
    return feature_map


def generate_title_maps(config):
    # create output folder if it doesn't exist
    end_date = config["DATES"]["END_DATE"]
    path = f"{config['BASE_PATH']['inventor']}".format(end_date=end_date) + config['INVENTOR_BUILD_TITLES']['feature_out']
    logging.info('writing results to folder: %s', os.path.dirname(path))
    # os.makedirs(os.path.dirname(config['INVENTOR_BUILD_TITLES']['feature_out']), exist_ok=True)
    features = dict()
    # If running incremental disambiguation
    if config['DISAMBIGUATION']['INCREMENTAL'] == "1":
        # Load latest full disambiguation results
        with open(config['INVENTOR_BUILD_TITLES']['base_title_map'], 'rb') as fin:
            features = pickle.load(fin)
    pool = mp.Pool()
    feats = [
        n for n in pool.starmap(
            build_title_map_for_source, [
                (config, 'granted_patent_database'),
                (config, 'pregrant_database')
            ])
    ]
    for i in range(0, len(feats)):
        features.update(feats[i])

    with open(path + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


def main(argv):
    logging.info('Building title features')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_title_map_sql.ini'])
    generate_title_maps(config)


if __name__ == "__main__":
    app.run(main)
