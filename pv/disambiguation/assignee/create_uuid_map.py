import configparser
import os
import pickle

from absl import app
from absl import logging
from tqdm import tqdm

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.util.config_util import generate_incremental_components

logging.set_verbosity(logging.INFO)


def create_uuid_map(config, source='granted_patent_database'):
    cnx_g = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    # query, record_id_format = pvdb.obtain_assignee_query(config=config, source=source)
    ignore_filters = config['DISAMBIGUATION'].get('debug', 0)
    components = generate_incremental_components(config, source, 'ra', ignore_filters)
    g_cursor = cnx_g.cursor()
    query = "SELECT {id_field}, {document_id_field}, {sequence_field} FROM {db}.rawassignee ra {filter};".format(
        id_field=components.get('id_field')
        , document_id_field=components.get('document_id_field')
        , sequence_field=components.get('sequence_field')
        , db=config['DISAMBIGUATION'][source]
        , filter=components.get('filter'))
    print(query)
    g_cursor.execute(query)
    uuids = dict()
    record_id_format = components.get('record_id_format')
    fmt = f"{record_id_format}-%s".format(record_id_format=record_id_format)
    for uuid, patent_id, seq, name_first, name_last, organization, type, rawlocation_id, city, state, country in tqdm(
            g_cursor, 'granted uuids'):
        uuids[fmt % (patent_id, seq)] = uuid
    return uuids


def generate_uuid_map(config):
    # if not os.path.exists(config['ASSIGNEE_UPLOAD']['uuidmap']):
    granted_uuids = create_uuid_map(config, 'granted_patent_database')
    pgranted_uuids = create_uuid_map(config, 'pregrant_database')
    with open(config['ASSIGNEE_UPLOAD']['uuidmap'], 'wb') as fout:
        pickle.dump([granted_uuids, pgranted_uuids], fout)


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/upload.ini'])
    generate_uuid_map(config)


if __name__ == "__main__":
    app.run(main)
