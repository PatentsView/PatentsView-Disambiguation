import os
import pickle

import mysql
import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm
import configparser

logging.set_verbosity(logging.INFO)
import pv.disambiguation.util.db as pvdb


def create_tables(config):
    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)
    g_cursor = cnx_g.cursor()
    g_cursor.execute(
        "CREATE TABLE tmp_location_disambiguation_granted2 (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute(
        "CREATE TABLE tmp_location_disambiguation_pregranted2 (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    g_cursor.close()
    pg_cursor.close()


def drop_tables(config):
    cnx_g = pvdb.granted_table(config)

    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute("DROP TABLE tmp_location_disambiguation_granted2")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute("DROP TABLE tmp_location_disambiguation_pregranted2")
    g_cursor.close()
    pg_cursor.close()


def create_uuid_map(config):
    cnx_g = pvdb.granted_table(config)

    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute("SELECT id FROM rawlocation;")
    granted_uuids = set()
    for uuid in tqdm(g_cursor, 'granted uuids'):
        granted_uuids.add(uuid[0])

    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute("SELECT id FROM rawlocation;")
    pgranted_uuids = set()
    for uuid in tqdm(pg_cursor, 'pregranted uuids'):
        pgranted_uuids.add(uuid[0])
    return granted_uuids, pgranted_uuids


def upload(granted_ids, pregranted_ids, config):
    logging.info('granted_ids size %s', len(granted_ids))
    logging.info('pregranted_ids size %s', len(pregranted_ids))

    pairs_pregranted = []
    pairs_granted = []
    with open(config['LOCATION_UPLOAD']['input'], 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if splt[0] in pregranted_ids:
                pairs_pregranted.append((splt[0], splt[1]))
            elif splt[0] in granted_ids:
                pairs_granted.append((splt[0], splt[1]))
            else:
                logging.warning('missing id %s', splt[0])
    logging.info('pairs granted size %s', len(pairs_granted))
    logging.info('pairs pregranted size %s', len(pairs_pregranted))

    cnx_g = pvdb.granted_table(config)

    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_granted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding granted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_granted), offsets[idx] + batch_size)
        sql = "INSERT INTO tmp_location_disambiguation_granted2 (uuid, disambiguated_id) VALUES " + ', '.join(
            ['("%s", "%s")' % x for x in pairs_granted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
    g_cursor.execute('alter table tmp_location_disambiguation_granted2 add primary key (uuid)')
    cnx_g.close()

    pg_cursor = cnx_pg.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_pregranted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding pregranted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_pregranted), offsets[idx] + batch_size)
        sql = "INSERT INTO tmp_location_disambiguation_pregranted2 (uuid, disambiguated_id) VALUES " + ', '.join(
            ['("%s", "%s")' % x for x in pairs_pregranted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        pg_cursor.execute(sql)
    cnx_pg.commit()
    pg_cursor.execute('alter table tmp_location_disambiguation_pregranted2 add primary key (uuid)')
    cnx_pg.close()


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/location/upload.ini'])

    if not os.path.exists(config['LOCATION_UPLOAD']['uuidmap']):
        granted_uuids, pgranted_uuids = create_uuid_map(config)
        with open(config['LOCATION_UPLOAD']['uuidmap'], 'wb') as fout:
            pickle.dump([granted_uuids, pgranted_uuids], fout)
    else:
        granted_uuids, pgranted_uuids = pickle.load(open(config['LOCATION_UPLOAD']['uuidmap'], 'rb'))

    upload(granted_uuids, pgranted_uuids, config)


if __name__ == "__main__":
    app.run(main)
