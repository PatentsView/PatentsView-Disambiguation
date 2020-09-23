import collections
import pickle

import mysql.connector
from absl import app
from absl import flags
from absl import logging

from pv.disambiguation.core import InventorMention

FLAGS = flags.FLAGS
flags.DEFINE_string('canopy_out', 'data/inventor/canopies', '')
flags.DEFINE_string('source', 'pregranted', 'pregranted or granted')

import os


def first_letter_last_name(im):
    fi = im.first_letter()[0] if len(im.first_letter()) > 0 else im.uuid
    lastname = im.last_name()[0] if len(im.last_name()) > 0 else im.uuid
    res = 'fl:%s_ln:%s' % (fi, lastname)
    return res


def build_pregrants():
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='pregrant_publications')
    cursor = cnx.cursor()
    query = "SELECT id, name_first, name_last FROM rawinventor;"
    cursor.execute(query)
    canopy2uuids = collections.defaultdict(list)
    idx = 0
    for uuid, name_first, name_last in cursor:
        im = InventorMention(uuid, '0', '', name_first if name_first else '', name_last if name_last else '', '', '',
                             '')
        canopy2uuids[first_letter_last_name(im)].append(uuid)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s canopies', 10000, idx, len(canopy2uuids))
    return canopy2uuids


def build_granted():
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                  database='patent_20200630')
    cursor = cnx.cursor()
    query = "SELECT uuid, name_first, name_last FROM rawinventor;"
    cursor.execute(query)
    canopy2uuids = collections.defaultdict(list)
    idx = 0
    for uuid, name_first, name_last in cursor:
        im = InventorMention(uuid, '0', '', name_first if name_first else '', name_last if name_last else '', '', '',
                             '')
        canopy2uuids[first_letter_last_name(im)].append(uuid)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s granted records - %s canopies', 10000, idx, len(canopy2uuids))
    return canopy2uuids


def main(argv):
    logging.info('Building canopies')
    if FLAGS.source == 'pregranted':
        canopies = build_pregrants()
    elif FLAGS.source == 'granted':
        canopies = build_granted()
    with open(FLAGS.canopy_out + '.%s.pkl' % FLAGS.source, 'wb') as fout:
        pickle.dump(canopies, fout)


if __name__ == "__main__":
    app.run(main)
