import configparser
import pickle

from absl import app
from absl import logging
from tqdm import tqdm

import pv.disambiguation.util.db as pvdb

logging.set_verbosity(logging.INFO)


def create_tables(config):
    cnx_g = pvdb.connect_to_disambiguation_database(config, dbtype='granted_patent_database')
    cnx_pg = pvdb.connect_to_disambiguation_database(config, dbtype='pregrant_database')

    g_cursor = cnx_g.cursor()
    table_name = config['ASSIGNEE_UPLOAD']['target_table']
    g_cursor.execute(
        "CREATE TABLE IF NOT EXISTS {table_name} (uuid VARCHAR(255), assignee_id VARCHAR(255))".format(
            table_name=table_name))
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute(
        "CREATE TABLE IF NOT EXISTS {table_name} (uuid VARCHAR(255), assignee_id VARCHAR(255))".format(
            table_name=table_name))
    g_cursor.close()
    pg_cursor.close()


def load_target_from_source(config, pairs, target='granted_patent_database'):
    cnx_g = pvdb.connect_to_disambiguation_database(config, dbtype=target)
    g_cursor = cnx_g.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding %s' % target, total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs), offsets[idx] + batch_size)
        sql = "INSERT INTO {table_name} (uuid, assignee_id) VALUES ".format(
            table_name=config['ASSIGNEE_UPLOAD']['target_table']) + ', '.join(
            ['("%s", "%s")' % x for x in pairs[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
    cnx_g.close()


def upload(config):
    granted_uuids, pgranted_uuids = pickle.load(open(config['ASSIGNEE_UPLOAD']['uuidmap'], 'rb'))
    pairs_pregranted = []
    pairs_granted = []
    finalize_output_file = "{}/disambiguation.tsv".format(config['assignee']['clustering_output_folder'])
    with open(finalize_output_file, 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if splt[0] in pgranted_uuids:
                pairs_pregranted.append((pgranted_uuids[splt[0]], splt[1]))
            elif splt[0] in granted_uuids:
                pairs_granted.append((granted_uuids[splt[0]], splt[1]))

    load_target_from_source(config, pairs_granted, target='granted_patent_database')
    load_target_from_source(config, pairs_pregranted, target='granted_patent_database')


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/upload.ini'])
    upload(config)


if __name__ == "__main__":
    app.run(main)
