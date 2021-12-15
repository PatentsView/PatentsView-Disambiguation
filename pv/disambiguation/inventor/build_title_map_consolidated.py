import configparser
import multiprocessing as mp
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
    incremental_components = generate_incremental_components(config, source, 'a')
    cursor = cnx.cursor(dictionary=True)
    query = "select {document_id_field},{title_field} from {title_table} a {filter};".format(
        document_id_field=incremental_components.get('document_id_field'),
        title_field=incremental_components.get('title_field'),
        filter=incremental_components.get('filter'),
        title_table=incremental_components.get('title_table'))
    cursor.execute(query)
    idx = 0
    for rec in cursor:
        record_id = incremental_components.get('record_id_format') % rec.get(
            incremental_components.get('document_id_field'))
        feature_map[record_id] = rec.get(incremental_components.get('title_field'))
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s %s records - %s features', 10000, source, idx, len(feature_map))
    logging.log(logging.INFO, 'Processed %s %s records - %s features', idx, source, len(feature_map))
    return feature_map


def generate_title_maps(config):
    # create output folder if it doesn't exist
    logging.info('writing results to folder: %s',
                 os.path.dirname(config['INVENTOR_BUILD_TITLES']['feature_out']))
    os.makedirs(os.path.dirname(config['INVENTOR_BUILD_TITLES']['feature_out']), exist_ok=True)
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

    with open(config['INVENTOR_BUILD_TITLES']['feature_out'] + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


def main(argv):
    logging.info('Building title features')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_title_map_sql.ini'])
    generate_title_maps(config)

if __name__ == "__main__":
    app.run(main)
