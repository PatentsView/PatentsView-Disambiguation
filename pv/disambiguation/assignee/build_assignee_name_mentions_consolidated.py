import collections
import billiard as mp
import os
import joblib
import pickle

from tqdm import tqdm
from absl import logging, app
import csv, sys
from pendulum import DateTime
from lib.configuration import get_disambig_config
from multiprocessing.pool import ThreadPool as Pool

from pv.disambiguation.util.config_util import generate_incremental_components


def csv_lines(filename, *args, **kwargs):
    delimiter = kwargs.get('delimiter', ",")
    quoting = kwargs.get('quoting', csv.QUOTE_NONNUMERIC)
    quotechar = kwargs.get('quotechar', '"')
    header = kwargs.get('header', False)
    if header:
        counter = -1
    else:
        counter = 0
    csv.field_size_limit(sys.maxsize)
    with open(filename) as fp:
        reader = csv.reader(fp, delimiter=delimiter, quotechar=quotechar, quoting=quoting)
        for line in reader:
            counter += 1
    return counter


def generate_assignee_records_from_sql(config, ignore_filters, source='granted_patent_database'):
    import pv.disambiguation.util.db as pvdb
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    incremental_components = generate_incremental_components(config, source, db_table_prefix='ra', ignore_filters=ignore_filters)
    query = """
    SELECT
        ra.{id_field}
      , ra.{document_id_field}
      , ra.{sequence_field} as sequence
      , name_first
      , name_last
      , organization
      , type
      , rawlocation_id
      , location_id
      -- , concat(ifnull(l.city, ""),ifnull(l.state, ""),ifnull(l.country,"")) as location_id
    FROM
        {db}.rawassignee ra
        left join {db}.rawlocation rl on rl.id=ra.rawlocation_id
        left join patent.location l on rl.location_id=l.id
        {filter}
        """.format(
        id_field=incremental_components.get('id_field'),
        document_id_field=incremental_components.get("document_id_field"),
        sequence_field=incremental_components.get('sequence_field'),
        db=config['DISAMBIGUATION'][source],
        filter=incremental_components.get('filter', 'where 1=1'),
        end_date=config["DATES"]["END_DATE"].replace("-", "")
        )
    cursor = cnx.cursor(dictionary=True)
    print(query)
    cursor.execute(query)
    for record in cursor:
        yield record


def build_assignee_mentions_for_source(config, source='granted_patent_database'):
    from pv.disambiguation.core import AssigneeMention
    ignore_filters = int(config['DISAMBIGUATION'].get('debug', 0))
    records_generator = generate_assignee_records_from_sql(config, ignore_filters, source)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in tqdm(records_generator, desc='Assignee NameMentions For Source', position=0, leave=True, file=sys.stdout, miniters=50000, maxinterval=600):
        am = AssigneeMention.from_sql_records(rec)
        feature_map[am.name_features()[0]].append(am)
        idx += 1
    return feature_map

def generate_assignee_mentions(config):
    from pv.disambiguation.core import AssigneeNameMention
    logging.info('Building assignee features')
    end_date = config["DATES"]["END_DATE_DASH"]
    path = f"{config['BASE_PATH']['assignee']}".format(end_date=end_date) + config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out']
    patent = build_assignee_mentions_for_source(config, 'granted_patent_database')
    pgpubs = build_assignee_mentions_for_source(config, 'pregrant_database')
    name_mentions = set(patent.keys()).union(set(pgpubs.keys()))
    feats = [patent, pgpubs]

    # logging.info('number of name mentions %s', len(name_mentions))
    records = dict()
    from collections import defaultdict
    canopies = defaultdict(set)
    pool = Pool(4)
    for nm in tqdm(name_mentions, desc='Assignee NameMentions', position=0, leave=True, file=sys.stdout, miniters=100000, maxinterval=1200):
        anm = AssigneeNameMention.from_assignee_mentions(nm, feats[0][nm] + feats[1][nm])
        for c in anm.canopies:
            pool.apply_async(canopies[c].add(anm.uuid), (c,))
        records[anm.uuid] = anm
    pool.close()
    pool.join()
    if os.path.isfile("assignee_mentions.records.pkl"):
        print("Removing Current File in Directory")
        os.remove("assignee_mentions.records.pkl")
    if os.path.isfile("assignee_mentions.canopies.pkl"):
        print("Removing Current File in Directory")
        os.remove("assignee_mentions.canopies.pkl")

    print(f"RECORDS HAVE SHAPE: {len(records.keys())}")
    print(f"CANOPIES HAVE SHAPE: {len(canopies.keys())}")

    with open(path + '.%s.pkl' % 'records', 'wb') as fout:
        pickle.dump(records, fout, buffer_callback=10, protocol=5)
    with open(path + '.%s.pkl' % 'canopies', 'wb') as fout:
        pickle.dump(canopies, fout, buffer_callback=10, protocol=5)


def main(argv):
    logging.info('Building assignee mentions')

    import configparser

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/build_name_mentions_sql.ini'])
    generate_assignee_mentions(config)


def pipeline_main():
    logging.info('Building assignee mentions')

    import configparser

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/build_name_mentions_sql.ini'])

    config['DATES'] = {'END_DATE': '2022-06-30'}
    generate_assignee_mentions(config)


if __name__ == "__main__":
    config = get_disambig_config(schedule='quarterly'
                                , supplemental_configs=['config/new_consolidated_config_ba.ini']
                                , **{'execution_date': DateTime(year=2022, month=7, day=1)})
    generate_assignee_mentions(config)
    # app.run(main)
    pipeline_main()
