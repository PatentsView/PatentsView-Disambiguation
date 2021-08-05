
import collections
import mysql.connector
from absl import logging
from tqdm import tqdm

from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_string('output', 'data/inventor/uuid2mentionid.tsv', '')
flags.DEFINE_string('source', 'pregranted', 'pregranted or granted')

import os
import configparser
import pv.disambiguation.util.db as pvdb



def build_granted(fout,config):
    cnx =  pvdb.granted_table(config)
    cursor = cnx.cursor()
    query = "SELECT uuid, patent_id, sequence FROM rawinventor;"
    cursor.execute(query)
    for uuid, patent_id, sequence in tqdm(cursor, 'process', total=17000000):
        fout.write('%s\t%s-%s\n' % (uuid, patent_id, sequence))

def main(argv):
    logging.info('Building uuid2mid')
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini'])

    with open(FLAGS.output, 'w') as fout:
        build_granted(fout, config)

if __name__ == "__main__":
    app.run(main)