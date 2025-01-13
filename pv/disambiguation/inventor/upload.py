import configparser
from datetime import datetime
import re
from absl import app
from absl import logging
from tqdm import tqdm

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.inventor import load_mysql

logging.set_verbosity(logging.INFO)
from mysql.connector.errors import ProgrammingError


def create_tables(config):
    cnx_g = pvdb.connect_to_disambiguation_database(config, dbtype='granted_patent_database')
    cnx_pg = pvdb.connect_to_disambiguation_database(config, dbtype='pregrant_database')
    g_cursor = cnx_g.cursor()
    table_name = config['INVENTOR_UPLOAD']['target_table']
    g_cursor.execute(
        "CREATE TABLE IF NOT EXISTS {table_name} (uuid VARCHAR(255), inventor_id VARCHAR(255))".format(
            table_name=table_name))
    pg_cursor = cnx_pg.cursor()
    pg_cursor.execute(
        "CREATE TABLE  IF NOT EXISTS {table_name}  (uuid VARCHAR(255), inventor_id VARCHAR(255))".format(
            table_name=table_name))
    g_cursor.close()
    pg_cursor.close()

def load_target_from_source(config, pairs, target='granted_patent_database'):
    cnx_g = pvdb.connect_to_disambiguation_database(config, dbtype=target)
    g_cursor = cnx_g.cursor()
    batch_size = 100000

    seen_uuids = set()
    unique_pairs = []
    duplicate_count = 0

    for pair in pairs:
        if pair[0] in seen_uuids:
            duplicate_count += 1  # Count duplicates
        else:
            seen_uuids.add(pair[0])
            unique_pairs.append(pair)

    print(f"Total pairs processed: {len(pairs)}")
    print(f"Unique pairs retained: {len(unique_pairs)}")
    print(f"Duplicate pairs filtered out: {duplicate_count}"

    unique_pairs = list({pair[0]: pair for pair in pairs}.values())
    offsets = [x for x in range(0, len(unique_pairs), batch_size)]
    print("INSERT INTO {table_name} (uuid, inventor_id) VALUES .... ".format(table_name=config['INVENTOR_UPLOAD']['target_table']) )
    for idx in tqdm(range(len(offsets)), 'adding %s' % target, total=len(offsets)):
        sidx = offsets[idx]
        eidx = min(len(pairs), offsets[idx] + batch_size)
        sql = """
        INSERT INTO {table_name} (uuid, inventor_id)
        VALUES {values}
        ON DUPLICATE KEY UPDATE uuid = uuid
        """.format(
            table_name=config['INVENTOR_UPLOAD']['target_table'],
            values=', '.join(['("%s", "%s")' % x for x in pairs[sidx:eidx]])
        )
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
    try:
        # Step 1: Add in Primary Key
        g_cursor.execute(
            'alter table {table_name} add primary key (uuid)'.format(
                table_name=config['INVENTOR_UPLOAD']['target_table']))
    except ProgrammingError as e:
        from mysql.connector import errorcode
        if not e.errno == errorcode.ER_MULTIPLE_PRI_KEY:
            raise
    cnx_g.close()


def upload(config):
    finalize_output_file = "{}/disambiguation.tsv".format(config['inventor']['clustering_output_folder'])

    # Use regex to extract the date in YYYYMMDD format
    match = re.search(r"(\d{8})",finalize_output_file)
    if match:
        original_date = match.group(1)  # Extract the matched date, e.g., "20241231"
        formatted_date = datetime.strptime(original_date, "%Y%m%d").strftime("%Y-%m-%d")  # Convert to "2024-12-31"

        # Replace the original date with the formatted date in the path
        finalize_output_file = finalize_output_file.replace(original_date, formatted_date)

    # Print the result
    print(finalize_output_file)

    loader = load_mysql.Loader.from_config(config)
    pregranted_ids = set([y for x in loader.pregranted_canopies.values() for y in x])
    granted_ids = set([y for x in loader.granted_canopies.values() for y in x])

    pairs_pregranted = []
    pairs_granted = []
    with open(finalize_output_file, 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if splt[0] in pregranted_ids:
                pairs_pregranted.append((splt[0], splt[1]))
            elif splt[0] in granted_ids:
                pairs_granted.append((splt[0], splt[1]))
    create_tables(config)
    # load_target_from_source(config, pairs_granted, target='granted_patent_database')
    load_target_from_source(config, pairs_pregranted, target='pregrant_database')


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/upload.ini', 'config/inventor/run_clustering.ini'])

    upload(config)


if __name__ == "__main__":
    app.run(main)
