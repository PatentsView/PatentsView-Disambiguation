from collections import defaultdict
# import configparser
import billiard as mp
import os
import pickle
import datetime

from absl import logging, app

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import InventorMention
from pv.disambiguation.util.config_util import generate_incremental_components
from pv.disambiguation.util.text_utils import last_name
from lib.configuration import get_disambig_config


def build_coperson_mentions_for_type(config, source='granted_patent_database') -> defaultdict :
    # create feature map dict to store results with default of []
    feature_map = defaultdict(list)
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    # if no DB section specified, no connection created and empty defaultdict returned
    if cnx is None:
        return feature_map
    ignore_filters = config['DISAMBIGUATION'].get('debug', 0)

    # get dict of field names and settings based on source setting
    incremental_components = generate_incremental_components(config, source,
                                                             db_table_prefix='ri', ignore_filters=ignore_filters)
    # NOTE: incremental_components["filter"] will either be a date-based filter or "WHERE 1=1"
    # | id | document_number | sequence | name_first | name_last | organization | type |
    # rawlocation_id | city | state | country | filename | created_date | updated_date |
    
    # NOTE: suffix (Jr, Sr, etc.) would be part of name_last and will get processed by the InventorMention class or names.last_name()

    # TODO:these record retrievals can be stored to prevent needing to query millions of records at multiple points in the disambiguation pipeline

    # retrieve all raw inventors from the specified DB - ~20M records for each doc type, 
    inv_query = """
        SELECT {id_field}, {document_id_field}, ri.name_first, ri.name_last
        FROM rawinventor ri 
        {filter}
    """.format(filter=incremental_components.get('filter'),
               id_field=incremental_components.get('id_field'),
               document_id_field=incremental_components.get('document_id_field'))
    
    # retrieve all *individual* assignees from the specified DB
    asgn_query = """
        SELECT {id_field}, {document_id_field}, ra.name_first, ra.name_last
        FROM rawassignee ra
        {filter}
    """.format(filter=incremental_components.get('filter'),
               id_field=incremental_components.get('id_field'),
               document_id_field=incremental_components.get('document_id_field'))

    # retrieve all *individual* applicants from the specified DB
    apl_query = """
        SELECT {id_field}, {document_id_field}, ra.name_first, ra.name_last
        FROM {apl_table} ra
        {filter}
        AND ra.name_last IS NOT NULL
        AND ra.organization IS NULL
    """.format(filter=incremental_components.get('filter'),
               id_field=incremental_components.get('id_field'),
               document_id_field=incremental_components.get('document_id_field'),
               apl_table= "us_parties" if incremental_components.get('document_id_field') == "document_number" else "non_inventor_applicant")

    for query in (inv_query, asgn_query, apl_query):
        cursor = cnx.cursor(dictionary=True) # each record is a dictionary, i.e. {field:value, field:value}
        print(query)
        cursor.execute(query)
        idx = 0
        rec: dict
        # is this only recording the raw ID and cleaned last name? seems like we could save some compute by just calling the last name cleaning directly
        for rec in cursor: # would this work with an enumerate?
            im = InventorMention.from_sql_record_dict(rec)
            feature_map[im.record_id].append(last_name(im))

            # import pv.disambiguation.inventor.names as names
            # rec_id = incremental_components.get("record_id_format", "%s").format(rec.get(incremental_components.get("document_id_field")))
            # feature_map[rec_id].append(names.last_name(rec.get("name_last")))

            idx += 1
            logging.log_every_n(logging.INFO, 'Processed %s %s records - %s features', 10000, source, idx,
                                len(feature_map))
    logging.log(logging.INFO, 'Processed %s %s  records - %s features', idx, source, len(feature_map))
    # feature map now looks like {'doc_id_1':['lastname_A','lastname_B'], 'doc_id_2':['lastname_C','lastname_D','lastname_E'], ... }
    return feature_map


def generate_coperson_mentions(config) -> None:
    """
    creates a dictionary with document IDs as keys and lists of surnames as values,
    then writes this dictionary to a pickle
    No Return
    """
    logging.info('Building assignee features')
    features = defaultdict(list)
    end_date = config["DATES"]["END_DATE_DASH"]
    # Generate mentions from granted and pregrant databases
    with mp.Pool() as pool:
        for feat in pool.starmap(
                build_coperson_mentions_for_type, [
                    (config, 'granted_patent_database'),
                    (config, 'pregrant_database')
                ]):
            features.update(feat)
            # features should now be the combination of two returned feature_maps from build_coperson_mentions_for_type:
            # {'doc_id_1':['lastname_A','lastname_B'], 'doc_id_2':['lastname_C','lastname_D','lastname_E'], ... }
            # containing both patent and pgpub doc_ids
    path = f"{config['BASE_PATH']['inventor']}".format(end_date=end_date) + config['INVENTOR_BUILD_COINVENTOR_FEAT']['feature_out']
    logging.info('writing results to folder: %s', os.path.dirname(path))
    if os.path.isfile(path + '.%s.pkl' % 'both'):
        print("Removing Current File in Directory")
        os.remove(path + '.%s.pkl' % 'both')
    with open(path + '.%s.pkl' % 'both', 'wb') as fout:
        pickle.dump(features, fout)


def main(argv):
    # config.read(['config/database_config.ini', 'config/database_tables.ini',
    #              'config/inventor/build_coinventor_features_sql.ini'])
    # config.read(['config/database_config.ini', 'config/database_tables.ini', 'config/inventor/build_coinventor_features_sql.ini'])
    config = get_disambig_config(schedule='quarterly',
                                 supplemental_configs=['config/consolidated_config.ini'],
                                 **{
                                     "execution_date": datetime.date(2022, 7, 1)
                                 })
    generate_coperson_mentions(config)


if __name__ == "__main__":
    app.run(main)
