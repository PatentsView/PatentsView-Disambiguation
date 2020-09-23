import os
import pickle

import mysql
import mysql.connector
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string('input', 'exp_out/assignee/run_22/disambiguation.tsv', '')
flags.DEFINE_string('uuidmap', 'data/assignee/uuid.pkl', '')

flags.DEFINE_boolean('create_tables', False, '')
flags.DEFINE_boolean('drop_tables', False, '')

logging.set_verbosity(logging.INFO)


def create_tables():
    cnx_g = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                    database='patent_20200630')
    cnx_pg = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                     database='pregrant_publications')

    g_cursor = cnx_g.cursor()
    g_cursor.execute(
        "CREATE TABLE tmp_assignee_disambiguation_granted (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute(
        "CREATE TABLE tmp_assignee_disambiguation_pregranted (uuid VARCHAR(255), disambiguated_id VARCHAR(255))")
    g_cursor.close()
    pg_cursor.close()


def drop_tables():
    cnx_g = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                    database='patent_20200630')
    cnx_pg = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                     database='pregrant_publications')

    g_cursor = cnx_g.cursor()
    g_cursor.execute("DROP TABLE tmp_assignee_disambiguation_granted")
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute("DROP TABLE tmp_assignee_disambiguation_pregranted")
    g_cursor.close()
    pg_cursor.close()


def create_uuid_map():
    cnx_g = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                    database='patent_20200630')
    cnx_pg = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                     database='pregrant_publications')

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


def upload(granted_ids, pregranted_ids):
    pairs_pregranted = []
    pairs_granted = []
    with open(FLAGS.input, 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if splt[0] in pregranted_ids:
                pairs_pregranted.append((pregranted_ids[splt[0]], splt[1]))
            elif splt[0] in granted_ids:
                pairs_granted.append((granted_ids[splt[0]], splt[1]))

    cnx_g = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                    database='patent_20200630')
    cnx_pg = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                     database='pregrant_publications')

    g_cursor = cnx_g.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_granted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding granted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_granted), offsets[idx] + batch_size)
        sql = "INSERT INTO tmp_assignee_disambiguation_granted (uuid, disambiguated_id) VALUES " + ', '.join(
            ['("%s", "%s")' % x for x in pairs_granted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
    g_cursor.execute('alter table tmp_assignee_disambiguation_granted add primary key (uuid)')
    cnx_g.close()

    pg_cursor = cnx_pg.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs_pregranted), batch_size)]
    for idx in tqdm(range(len(offsets)), 'adding pregranted', total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs_pregranted), offsets[idx] + batch_size)
        sql = "INSERT INTO tmp_assignee_disambiguation_pregranted (uuid, disambiguated_id) VALUES " + ', '.join(
            ['("%s", "%s")' % x for x in pairs_pregranted[sidx:eidx]])
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        pg_cursor.execute(sql)
    cnx_pg.commit()
    pg_cursor.execute('alter table tmp_assignee_disambiguation_pregranted add primary key (uuid)')
    cnx_pg.close()


def main(argv):
    # if FLAGS.drop_tables:
    #     drop_tables()

    if FLAGS.create_tables:
        create_tables()

    if not os.path.exists(FLAGS.uuidmap):
        granted_uuids, pgranted_uuids = create_uuid_map()
        with open(FLAGS.uuidmap, 'wb') as fout:
            pickle.dump([granted_uuids, pgranted_uuids], fout)
    else:
        granted_uuids, pgranted_uuids = pickle.load(open(FLAGS.uuidmap, 'rb'))

    upload(granted_uuids, pgranted_uuids)


if __name__ == "__main__":
    app.run(main)
