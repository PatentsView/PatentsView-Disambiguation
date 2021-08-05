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


def create_tables(config):
    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute(
        "CREATE TABLE temp_assignee_disambiguation_mapping (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute(
        "CREATE TABLE temp_assignee_disambiguation_mapping (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    g_cursor.close()
    pg_cursor.close()


def drop_tables(config):
    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute("TRUNCATE TABLE temp_assignee_disambiguation_mapping")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute("TRUNCATE TABLE temp_assignee_disambiguation_mapping")
    g_cursor.close()
    pg_cursor.close()


def create_uuid_map(config):
    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute("SELECT uuid, patent_id, sequence FROM rawassignee;")
    granted_uuids = dict()
    for uuid, patent_id, seq in tqdm(g_cursor, 'granted uuids'):
        granted_uuids['%s-%s' % (patent_id, seq)] = uuid

    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute("SELECT id, document_number, sequence-1 as sequence FROM rawassignee;")
    pgranted_uuids = dict()
    for uuid, doc_id, seq in tqdm(pg_cursor, 'pregranted uuids'):
        pgranted_uuids['pg-%s-%s' % (doc_id, seq)] = uuid
    return granted_uuids, pgranted_uuids


def upload(granted_ids, pregranted_ids, config):
    pairs_pregranted = []
    pairs_granted = []
    with open(config['ASSIGNEE_UPLOAD']['input'], 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if splt[0] in pregranted_ids:
                pairs_pregranted.append((pregranted_ids[splt[0]], splt[1]))
            elif splt[0] in granted_ids:
                pairs_granted.append((granted_ids[splt[0]], splt[1]))

    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_granted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding granted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_granted), offsets[idx] + batch_size)
        sql = "INSERT INTO temp_assignee_disambiguation_mapping (uuid,  assignee_id, version_indicator) VALUES " + ', '.join(
            ['("%s", "%s", "20201229")' % x for x in pairs_granted[sidx:eidx]])
        #logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
#    g_cursor.execute('alter table temp_assignee_disambiguation_mapping add primary key (uuid)')
    cnx_g.close()

    pg_cursor = cnx_pg.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_pregranted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding pregranted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_pregranted), offsets[idx] + batch_size)
        sql = "INSERT INTO temp_assignee_disambiguation_mapping (uuid, assignee_id, version_indicator) VALUES " + ', '.join(
            ['("%s", "%s", "20201229")' % x for x in pairs_pregranted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        pg_cursor.execute(sql)
    cnx_pg.commit()
#    pg_cursor.execute('alter table temp_assignee_disambiguation_mapping add primary key (uuid)')
    cnx_pg.close()


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/upload.ini'])

    if not os.path.exists(config['ASSIGNEE_UPLOAD']['uuidmap']):
        granted_uuids, pgranted_uuids = create_uuid_map(config)
        with open(config['ASSIGNEE_UPLOAD']['uuidmap'], 'wb') as fout:
            pickle.dump([granted_uuids, pgranted_uuids], fout)
    else:
        granted_uuids, pgranted_uuids = pickle.load(open(config['ASSIGNEE_UPLOAD']['uuidmap'], 'rb'))

    upload(granted_uuids, pgranted_uuids, config)


if __name__ == "__main__":
    app.run(main)
