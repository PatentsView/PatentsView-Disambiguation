import collections
import billiard as mp
import os
import pickle
from tqdm import tqdm
from absl import logging, app
import csv, sys

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
    FROM
        {db}.rawassignee ra
        left join  {db}.rawlocation rl on rl.id=ra.rawlocation_id
        # assuming this join is here to only bring in records with accurate country code attributes; BUT REMOVES ~77k records (almost 10%)
        # inner join geo_data.country_codes cc on cc.`alpha-2`=rl.country_transformed
        {filter}
        """.format(
        id_field=incremental_components.get('id_field'),
        document_id_field=incremental_components.get("document_id_field"),
        sequence_field=incremental_components.get('sequence_field'),
        db=config['DISAMBIGUATION'][source],
        filter=incremental_components.get('filter', 'where 1=1'),
        end_date=config["DATES"]["END_DATE"].replace("-","")
        )
    cursor = cnx.cursor(dictionary=True)
    print(query)
    cursor.execute(query)
    for record in cursor:
        yield record


def generate_assignee_records_from_csv(source='granted_patent_database'):
    filename = "experiments/granted_source.csv"
    if source == 'pregrant_database':
        filename = "experiments/pregranted_source.csv"
    total_lines = csv_lines(filename)
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        for row in tqdm(reader, total=total_lines):
            yield row


def build_assignee_mentions_for_source(config, source='granted_patent_database'):
    from pv.disambiguation.core import AssigneeMention
    ignore_filters = int(config['DISAMBIGUATION'].get('debug', 0))
    # cursor = generate_assignee_records_from_csv(config, ignore_filters=ignore_filters, source=source)
    records_generator = generate_assignee_records_from_sql(config, ignore_filters, source)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in tqdm(records_generator):
        am = AssigneeMention.from_sql_records(rec)
        feature_map[am.name_features()[0]].append(am)
        idx += 1
        # logging.log_every_n(logging.INFO, 'Processed %s %s records - %s features', 10000, source, idx, len(feature_map))
    return feature_map


def generate_assignee_mentions(config):
    from pv.disambiguation.core import AssigneeNameMention
    logging.info('Building assignee features')
    end_date = config["DATES"]["END_DATE"]
    path = f"{config['BASE_PATH']['assignee']}".format(end_date=end_date) + config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out']
    print(path)
    # Generate mentions from granted and pregrant databases
    pool = mp.Pool()
    feats = [
        n for n in pool.starmap(
            build_assignee_mentions_for_source, [
                (config, 'granted_patent_database'),
                (config, 'pregrant_database')
            ])
    ]
    name_mentions = set(feats[0].keys()).union(set(feats[1].keys()))
    # logging.info('number of name mentions %s', len(name_mentions))
    records = dict()
    from collections import defaultdict
    canopies = defaultdict(set)
    name_mentions_progress = tqdm.tqdm(total=len(name_mentions.keys()), desc='Assignee NameMentions', position=0)
    for nm in name_mentions:
        anm = AssigneeNameMention.from_assignee_mentions(nm, feats[0][nm] + feats[1][nm])
        for c in anm.canopies:
            canopies[c].add(anm.uuid)
        records[anm.uuid] = anm
        name_mentions_progress.update(1)
    if os.path.isfile("assignee_mentions.records.pkl"):
        print("Removing Current File in Directory")
        os.remove("assignee_mentions.records.pkl")
    if os.path.isfile("assignee_mentions.canopies.pkl"):
        print("Removing Current File in Directory")
        os.remove("assignee_mentions.canopies.pkl")
    with open(path + '.%s.pkl' % 'records', 'wb') as fout:
        pickle.dump(records, fout)
    with open(path + '.%s.pkl' % 'canopies', 'wb') as fout:
        pickle.dump(canopies, fout)


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
    # app.run(main)
    pipeline_main()
