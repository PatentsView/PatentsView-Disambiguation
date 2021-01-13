import os

import mysql
import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm
import configparser

from pv.disambiguation.inventor import load_mysql
import pv.disambiguation.util.db as pvdb

logging.set_verbosity(logging.INFO)


def create_tables(config):
    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    g_cursor.execute(
        "CREATE TABLE tmp_inventor_disambiguation_granted2 (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute(
        "CREATE TABLE tmp_inventor_disambiguation_pregranted2 (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    g_cursor.close()
    pg_cursor.close()


def upload(config):
    loader = load_mysql.Loader.from_config(config)
    pregranted_ids = set([y for x in loader.pregranted_canopies.values() for y in x])
    granted_ids = set([y for x in loader.granted_canopies.values() for y in x])

    disamb = dict()
    with open(config['INVENTOR_UPLOAD']['input'], 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if len(splt) != 2:
                print('error %s' % str(splt))
            else:
                disamb[splt[0]] = splt[1]


    pairs_pregranted = []
    pairs_granted = []
    with open(config['INVENTOR_UPLOAD']['input'], 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if splt[0] in pregranted_ids:
                pairs_pregranted.append((splt[0], splt[1]))
            elif splt[0] in granted_ids:
                pairs_granted.append((splt[0], splt[1]))

    cnx_g = pvdb.granted_table(config)
    cnx_pg = pvdb.pregranted_table(config)

    g_cursor = cnx_g.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_granted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding granted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_granted), offsets[idx] + batch_size)
        sql = "INSERT INTO tmp_inventor_disambiguation_granted2 (uuid, disambiguated_id) VALUES " + ', '.join(
            ['("%s", "%s")' % x for x in pairs_granted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
    g_cursor.execute('alter table tmp_inventor_disambiguation_granted2 add primary key (uuid)')
    cnx_g.close()

    pg_cursor = cnx_pg.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_pregranted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding pregranted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_pregranted), offsets[idx] + batch_size)
        sql = "INSERT INTO tmp_inventor_disambiguation_pregranted2 (uuid, disambiguated_id) VALUES " + ', '.join(
            ['("%s", "%s")' % x for x in pairs_pregranted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        pg_cursor.execute(sql)
    cnx_pg.commit()
    pg_cursor.execute('alter table tmp_inventor_disambiguation_pregranted2 add primary key (uuid)')
    cnx_pg.close()


def main(argv):

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/upload.ini', 'config/inventor/run_clustering.ini'])

    upload(config)


if __name__ == "__main__":
    app.run(main)
