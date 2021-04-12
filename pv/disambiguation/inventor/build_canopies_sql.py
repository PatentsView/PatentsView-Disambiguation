import collections
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging
import configparser

from pv.disambiguation.core import InventorMention
import pv.disambiguation.util.db as pvdb

import os


def first_letter_last_name(im):
    fi = im.first_letter()[0] if len(im.first_letter()) > 0 else im.uuid
    lastname = im.last_name()[0] if len(im.last_name()) > 0 else im.uuid
    res = 'fl:%s_ln:%s' % (fi, lastname)
    return res


def build_pregrants(config):
    canopy2uuids = collections.defaultdict(list)
    cnx = pvdb.pregranted_table(config)
    # cnx is none if we haven't specified a pregranted table
    if cnx is None:
        return canopy2uuids
    cursor = cnx.cursor()
    query = "SELECT id, name_first, name_last FROM rawinventor;"
    cursor.execute(query)
    idx = 0
    for uuid, name_first, name_last in cursor:
        im = InventorMention(uuid, '0', '', name_first if name_first else '', name_last if name_last else '', '', '',
                             '')
        canopy2uuids[first_letter_last_name(im)].append(uuid)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s canopies', 10000, idx, len(canopy2uuids))
    logging.log(logging.INFO, 'Processed %s pregrant records - %s canopies', idx, len(canopy2uuids))

    return canopy2uuids


def build_granted(config):
    canopy2uuids = collections.defaultdict(list)

    cnx = pvdb.granted_table(config)
    # cnx is none if we haven't specified a granted table
    if cnx is None:
        return canopy2uuids
    cursor = cnx.cursor()
    query = "SELECT uuid, name_first, name_last FROM rawinventor;"
    cursor.execute(query)
    idx = 0
    for uuid, name_first, name_last in cursor:
        im = InventorMention(uuid, '0', '', name_first if name_first else '', name_last if name_last else '', '', '',
                             '')
        canopy2uuids[first_letter_last_name(im)].append(uuid)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s granted records - %s canopies', 10000, idx, len(canopy2uuids))
    logging.log(logging.INFO, 'Processed %s granted records - %s canopies', idx, len(canopy2uuids))
    return canopy2uuids


def main(argv):
    logging.info('Building canopies')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_canopies_sql.ini'])

    # create output folder if it doesn't exist
    logging.info('writing results to folder: %s',
                 os.path.dirname(config['INVENTOR_BUILD_CANOPIES']['canopy_out']))
    os.makedirs(os.path.dirname(config['INVENTOR_BUILD_CANOPIES']['canopy_out']), exist_ok=True)

    canopies = build_pregrants(config)
    with open(config['INVENTOR_BUILD_CANOPIES']['canopy_out'] + '.%s.pkl' % 'pregranted', 'wb') as fout:
        pickle.dump(canopies, fout)

    canopies = build_granted(config)
    with open(config['INVENTOR_BUILD_CANOPIES']['canopy_out'] + '.%s.pkl' % 'granted', 'wb') as fout:
        pickle.dump(canopies, fout)


if __name__ == "__main__":
    app.run(main)
