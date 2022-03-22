import collections
import multiprocessing as mp
import pickle

from absl import logging, app


def build_assignee_mentions_for_source(config, source='granted_patent_database'):
    import pv.disambiguation.util.db as pvdb
    from pv.disambiguation.core import AssigneeMention
    from pv.disambiguation.util.config_util import generate_incremental_components
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    cursor = cnx.cursor(dictionary=True)
    incremental_components = generate_incremental_components(config, source,
                                                             db_table_prefix='ra')
    query = """
    SELECT ra.{id_field}, ra.{document_id_field}, ra.{sequence_field} as sequence, name_first,
     name_last, organization, type, rawlocation_id, rl.city, rl.state, rl.country
      FROM db.rawassignee ra left join rawlocation rl on rl.id = ra.rawlocation_id
    """.format(
        id_field=incremental_components.get('id_field'),
        document_id_field=incremental_components.get("document_id_field"),
        sequence_field=incremental_components.get('sequence_field'),
        db=config['DISAMBIGUATION'][source])
    print(query)
    cursor.execute(query)
    feature_map = collections.defaultdict(list)
    idx = 0
    for rec in cursor:
        am = AssigneeMention.from_sql_records(rec)
        feature_map[am.name_features()[0]].append(am)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s pregrant records - %s features', 10000, idx, len(feature_map))
    return feature_map


def generate_assignee_mentions(config):
    from pv.disambiguation.core import AssigneeNameMention
    logging.info('Building assignee features')
    features = collections.defaultdict(list)
    # Generate mentions from granted and pregrant databases
    pool = mp.Pool()
    feats = [
        n for n in pool.starmap(
            build_assignee_mentions_for_source, [
                (config, 'granted_patent_database'),
                (config, 'pregrant_database')
            ])
    ]
    logging.info('finished loading mentions %s', len(feats))
    name_mentions = set(feats[0].keys()).union(set(feats[1].keys()))
    logging.info('number of name mentions %s', len(name_mentions))
    from tqdm import tqdm
    records = dict()
    from collections import defaultdict
    canopies = defaultdict(set)
    for nm in tqdm(name_mentions, 'name_mentions'):
        anm = AssigneeNameMention.from_assignee_mentions(nm, feats[0][nm] + feats[1][nm])
        for c in anm.canopies:
            canopies[c].add(anm.uuid)
        records[anm.uuid] = anm

    with open(config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out'] + '.%s.pkl' % 'records', 'wb') as fout:
        pickle.dump(records, fout)
    with open(config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out'] + '.%s.pkl' % 'canopies', 'wb') as fout:
        pickle.dump(canopies, fout)


def main(argv):
    logging.info('Building assignee mentions')

    import configparser
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/assignee/build_name_mentions_sql.ini'])
    generate_assignee_mentions(config)


if __name__ == "__main__":
    app.run(main)
