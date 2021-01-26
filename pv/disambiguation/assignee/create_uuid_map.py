import os
import pickle

import mysql
import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm
import configparser

import pv.disambiguation.util.db as pvdb


logging.set_verbosity(logging.INFO)




def create_uuid_map(config):
    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute("SELECT uuid, patent_id, sequence FROM rawassignee;")
    granted_uuids = dict()
    for uuid, patent_id, seq in tqdm(g_cursor, 'granted uuids'):
        granted_uuids['%s-%s' % (patent_id, seq)] = uuid

    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute("SELECT id, document_number, sequence FROM rawassignee;")
    pgranted_uuids = dict()
    for uuid, doc_id, seq in tqdm(pg_cursor, 'pregranted uuids'):
        pgranted_uuids['pg-%s-%s' % (doc_id, seq)] = uuid
    return granted_uuids, pgranted_uuids



def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/upload.ini'])

    if not os.path.exists(config['ASSIGNEE_UPLOAD']['uuidmap']):
        granted_uuids, pgranted_uuids = create_uuid_map(config)
        with open(config['ASSIGNEE_UPLOAD']['uuidmap'], 'wb') as fout:
            pickle.dump([granted_uuids, pgranted_uuids], fout)


if __name__ == "__main__":
    app.run(main)
