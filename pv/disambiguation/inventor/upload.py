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
    print("inside load target from source")
    print(len(pairs))
    cnx_g = pvdb.connect_to_disambiguation_database(config, dbtype=target)
    g_cursor = cnx_g.cursor()
    batch_size = 100000
    offsets = [x for x in range(0, len(pairs), batch_size)]
    print("length of offsets")
    print(len(offsets))
    try:
        g_cursor.execute(
            'alter table {table_name} add primary key (uuid)'.format(
                table_name=config['INVENTOR_UPLOAD']['target_table']))
    except ProgrammingError as e:
        from mysql.connector import errorcode
        if not e.errno == errorcode.ER_MULTIPLE_PRI_KEY:
            raise
    print("added primary key to table")
    cnx_g.commit()
    print("INSERT INTO {table_name} (uuid, inventor_id) VALUES .... ".format(table_name=config['INVENTOR_UPLOAD']['target_table']) )
    for idx in tqdm(range(len(offsets)), 'adding %s' % target, total=len(offsets), mininterval=(len(offsets)/100)):
        sidx = offsets[idx]
        eidx = min(len(pairs), offsets[idx] + batch_size)
        sql = """
        INSERT INTO {table_name} (uuid, inventor_id) VALUES 
        """.format(
            table_name=config['INVENTOR_UPLOAD']['target_table']
        ) + ', '.join(
            ['(%s, %s)' % x for x in pairs[sidx:eidx]]
        ) + """
        ON DUPLICATE KEY UPDATE inventor_id = VALUES(inventor_id)
        """
        # logging.log_first_n(logging.INFO, '%s', 1, sql)
        g_cursor.execute(sql)
    cnx_g.commit()
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

    # Print the lengths of the sets
    print("Length of pregranted_ids:", len(pregranted_ids))
    print("Length of granted_ids:", len(granted_ids))

    pairs_pregranted = []
    pairs_granted = []
    with open(finalize_output_file, 'r') as fin:
        line_count = 0  # Counter to limit the number of lines printed
        print_limit = 5  # Change this to the number of lines you want to preview

        for line in fin:
            splt = line.strip().split('\t')

            # Print the line and the split content for the first few lines
            if line_count < print_limit:
                print(f"Raw line: {line.strip()}")
                print(f"Split content: {splt}")

            if splt[0] in pregranted_ids:
                pairs_pregranted.append((splt[0], splt[1]))
            elif splt[0] in granted_ids:
                pairs_granted.append((splt[0], splt[1]))

            line_count += 1

    print(f"pre set length of pair_granted:{len(pairs_granted)}")
    print(f"pre set length of pair_pregranted:{len(pairs_pregranted)}")

    # Convert pairs_granted and pairs_pregranted to sets to remove duplicates
    pairs_granted = list(set(pairs_granted))
    pairs_pregranted = list(set(pairs_pregranted))

    # Log the lengths after removing duplicates
    print(f" After set Length of unique pairs_granted: {len(pairs_granted)}")
    print(f"After set Length of unique pairs_pregranted: {len(pairs_pregr

    create_tables(config)
    load_target_from_source(config, pairs_granted, target='granted_patent_database')
    load_target_from_source(config, pairs_pregranted, target='pregrant_database')


def main(argv):
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/upload.ini', 'config/inventor/run_clustering.ini'])

    upload(config)


if __name__ == "__main__":
    app.run(main)
