
import collections
import mysql.connector
from er.patents.core import InventorMention
from absl import logging
import pickle
from tqdm import tqdm

from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_string('output', 'data/inventor/uuid2mentionid.tsv', '')
flags.DEFINE_string('source', 'pregranted', 'pregranted or granted')

import os


def build_granted(fout):
    cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'],'.mylogin.cnf'), database='patent_20200630')
    cursor = cnx.cursor()
    query = "SELECT uuid, patent_id, sequence FROM rawinventor;"
    cursor.execute(query)
    for uuid, patent_id, sequence in tqdm(cursor, 'process', total=17000000):
        fout.write('%s\t%s-%s\n' % (uuid, patent_id, sequence))

def main(argv):
    logging.info('Building uuid2mid')
    with open(FLAGS.output, 'w') as fout:
        build_granted(fout)

if __name__ == "__main__":
    app.run(main)