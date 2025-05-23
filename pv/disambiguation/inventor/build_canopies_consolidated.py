import collections
import configparser
import os
import pickle


from absl import logging, app
from tqdm import tqdm

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import InventorMention
from pv.disambiguation.util.config_util import generate_incremental_components


def first_letter_last_name(im, num_first_letters=2):
    if num_first_letters == 1:
        first = im.first_letter()[0] if len(im.first_letter()) > 0 else im.uuid
    elif num_first_letters == 2:
        first = im.first_two_letters()[0] if len(im.first_two_letters()) > 0 else im.uuid

    lastname = im.last_name()[0] if len(im.last_name()) > 0 else im.uuid
    lastname = lastname.replace(' ', '')
    lastname = lastname.replace('-', '')
    res = 'fl:%s_ln:%s' % (first, lastname)
    return res


def build_canopies_for_type(config, source='granted_patent_database'):
    canopy2uuids = collections.defaultdict(list)
    cnx = pvdb.connect_to_disambiguation_database(config, dbtype=source)
    ignore_filters = config['DISAMBIGUATION'].get('debug', 0)

    incremental_components = generate_incremental_components(config, source, db_table_prefix='ri', ignore_filters=ignore_filters)
    # cnx is none if we haven't specified a pregranted table
    if cnx is None:
        return canopy2uuids
    cursor = cnx.cursor()
    # TODO: See about storing and re-loading these records from an earlier step
    query = "SELECT {id_field}, name_first, name_last FROM rawinventor ri {filter};" \
        .format(id_field=incremental_components.get('id_field'),
                filter=incremental_components.get('filter'))
    # TODO: repeat for individual assignees and inventors
    print(query)
    cursor.execute(query)
    idx = 0
    for uuid, name_first, name_last in cursor:
        im = InventorMention(uuid, '0', '', name_first if name_first else '', name_last if name_last else '', '', '',
                             '')
        canopy2uuids[first_letter_last_name(im, num_first_letters=2)].append(uuid)
        idx += 1
        logging.log_every_n(logging.INFO, 'Processed %s %s records - %s canopies', 1000, source, idx,
                            len(canopy2uuids))
    logging.log(logging.INFO, 'Processed %s %s records - %s canopies', idx, source, len(canopy2uuids))
    # canopy2uuids has structure {"fl:ab_ln:cdefg":['uuid-1','uuid-2'], ... }
    return canopy2uuids


# def supplement_from_the_past(config, new_granted_canopies, new_pregranted_canopies):
#     with open(config['INVENTOR_BUILD_CANOPIES']['base_pregranted_canopies'], 'rb') as fin:
#         pregranted_canopies = pickle.load(fin)
#     with open(config['INVENTOR_BUILD_CANOPIES']['base_granted_canopies'], 'rb') as fin:
#         granted_canopies = pickle.load(fin)
#
#     newkeys_pregranted = set([k for k in new_pregranted_canopies.keys()])
#     newkeys_granted = set([k for k in new_granted_canopies.keys()])
#     newkeys_granted_filtered = [x for x in newkeys_granted if x not in newkeys_pregranted]
#
#     for k in tqdm(newkeys_pregranted, 'setting up pregranted'):
#         if k in pregranted_canopies:
#             new_pregranted_canopies[k].extend(pregranted_canopies[k])
#         if k in granted_canopies:
#             new_granted_canopies[k].extend(granted_canopies[k])
#
#     for k in tqdm(newkeys_granted_filtered, 'setting up granted'):
#         if k in pregranted_canopies:
#             new_pregranted_canopies[k].extend(pregranted_canopies[k])
#         if k in granted_canopies:
#             new_granted_canopies[k].extend(granted_canopies[k])
#     return new_granted_canopies, new_pregranted_canopies


def generate_inventor_canopies(config) -> None:
    """
    creates two dictionaries with inventor name patterns as keys and lists of raw record uuids as values,
    then writes these dictionaries to pickles, one for pregrant, one for granted.
    No Return
    """
    # create output folder if it doesn't exist
    end_date = config["DATES"]["END_DATE_DASH"]
    path = f"{config['BASE_PATH']['inventor']}".format(end_date=end_date) + config['INVENTOR_BUILD_CANOPIES']['canopy_out']
    logging.info('writing results to files: %s', os.path.dirname(path))

    new_pregranted_canopies = build_canopies_for_type(config, source='pregrant_database')
    new_granted_canopies = build_canopies_for_type(config, source='granted_patent_database')

    # if config['DISAMBIGUATION']['INCREMENTAL'] == "1":
    #     new_granted_canopies, new_pregranted_canopies = supplement_from_the_past(config,
    #                                                                              new_granted_canopies,
    #                                                                              new_pregranted_canopies)
    # elif config['DISAMBIGUATION']['INCREMENTAL'] == "0":
    # Dropping pickle files for recreation
    #     print(f"WARNING -- DELETING PRIOR PKL FILES AT {path} FOR REGENERATION")
    if os.path.isfile(path + '.%s.pkl' % 'pregranted'):
        print("Removing Current File in Directory")
        os.remove(path + '.%s.pkl' % 'pregranted')
    if os.path.isfile(path + '.%s.pkl' % 'granted'):
        print("Removing Current File in Directory")
        os.remove(path + '.%s.pkl' % 'granted')

    # Export pickle files
    with open(path + '.%s.pkl' % 'pregranted', 'wb') as fout:
        pickle.dump(new_pregranted_canopies, fout)

    with open(path + '.%s.pkl' % 'granted', 'wb') as fout:
        pickle.dump(new_granted_canopies, fout)


def main(argv):
    logging.info('Building canopies')

    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini',
                 'config/inventor/build_canopies_sql.ini'])
    generate_inventor_canopies(config)


if __name__ == "__main__":
    app.run(main)
    # first_letter_last_name("Danny, Bushelman")
