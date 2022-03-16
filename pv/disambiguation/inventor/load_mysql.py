import pickle

from absl import logging

import pv.disambiguation.util.db as pvdb
from pv.disambiguation.core import InventorMention


class Loader(object):
    def __init__(self, pregranted_canopies, granted_canopies, config):
        self.pregranted_canopies = pregranted_canopies
        self.granted_canopies = granted_canopies
        self.cnx_g = pvdb.granted_table(config)
        self.cnx_pg = pvdb.pregranted_table(config)
        self.cnx_g_inc = pvdb.connect_to_disambiguation_database(config,dbtype='granted_patent_database')
        self.cnx_pg_inc = pvdb.connect_to_disambiguation_database(config,dbtype='pregrant_database')

    def load(self, canopy):
        return load_canopy(canopy,
                           self.pregranted_canopies[canopy] if canopy in self.pregranted_canopies else [],
                           self.granted_canopies[canopy] if canopy in self.granted_canopies else [],
                           self.cnx_pg, self.cnx_g, self.cnx_pg_inc, self.cnx_g_inc)

    def ids_for(self, canopies):
        return [x for canopy in canopies for x in
                (self.pregranted_canopies[canopy] if canopy in self.pregranted_canopies else [])], \
               [x for canopy in canopies for x in
                (self.granted_canopies[canopy] if canopy in self.granted_canopies else [])]

    def load_canopies(self, canopies):
        return load_canopy('batch of %s' % len(canopies),
                           [x for canopy in canopies for x in
                            (self.pregranted_canopies[canopy] if canopy in self.pregranted_canopies else [])],
                           [x for canopy in canopies for x in
                            (self.granted_canopies[canopy] if canopy in self.granted_canopies else [])],
                           self.cnx_pg, self.cnx_g, self.cnx_pg_inc, self.cnx_g_inc)

    def num_records(self, canopy):
        return len(self.pregranted_canopies[canopy] if canopy in self.pregranted_canopies else []) + len(
            self.granted_canopies[canopy] if canopy in self.granted_canopies else [])

    @staticmethod
    def from_flags(flgs):
        with open(flgs.pregranted_canopies, 'rb') as fin:
            pregranted_canopies = pickle.load(fin)
        with open(flgs.granted_canopies, 'rb') as fin:
            granted_canopies = pickle.load(fin)
        l = Loader(pregranted_canopies, granted_canopies)
        return l

    @staticmethod
    def from_config(config, config_type='inventor'):
        logging.info('building loader from config %s', str(config))
        if config[config_type]['pregranted_canopies'].lower() != 'none':
            logging.info('loading pregranted canopies from %s', config[config_type]['pregranted_canopies'])
            with open(config[config_type]['pregranted_canopies'], 'rb') as fin:
                pregranted_canopies = pickle.load(fin)
        else:
            logging.info('using no pregranted canopies')
            pregranted_canopies = set()
        if config[config_type]['granted_canopies'].lower() != 'none':
            logging.info('loading granted canopies from %s', config[config_type]['granted_canopies'])
            with open(config[config_type]['granted_canopies'], 'rb') as fin:
                granted_canopies = pickle.load(fin)
        else:
            logging.info('using no granted canopies')
            granted_canopies = set()
        l = Loader(pregranted_canopies, granted_canopies, config)
        return l

def load_canopy(canopy_name, pregrant_ids, granted_ids, cnx_pg, cnx_g, cnx_pg_inc=None, cnx_g_inc=None):
    logging.info('Loading data from canopy %s, %s pregranted, %s granted', canopy_name, len(pregrant_ids),
                 len(granted_ids))
    rec = (get_pregrants(pregrant_ids, cnx_pg) if pregrant_ids else []) + (
        get_granted(granted_ids, cnx_g) if granted_ids else [])
    # if cnx_pg_inc is not None:
    #     rec.extend(get_pregrants(pregrant_ids, cnx_pg_inc) if pregrant_ids else [])
    # if cnx_g_inc is not None:
    #     rec.extend(get_granted(granted_ids, cnx_g_inc) if pregrant_ids else [])
    return rec


def get_granted(ids, cnx, max_query_size=300000):
    # | uuid | patent_id | assignee_id | rawlocation_id | type | name_first | name_last | organization | sequence |
    cursor = cnx.cursor()
    feature_map = dict()
    for idx in range(0, len(ids), max_query_size):
        id_str = ", ".join(['"%s"' % x for x in ids[idx:idx + max_query_size]])
        query = "SELECT uuid, patent_id, inventor_id, rawlocation_id, name_first, name_last, sequence, rule_47, deceased FROM rawinventor WHERE uuid in (%s);" % id_str
        cursor.execute(query)
        for rec in cursor:
            am = InventorMention.from_granted_sql_record(rec)
            feature_map[am.uuid] = am
    missed = [x for x in ids if x not in feature_map]
    logging.warning('[get_granted] got: %s, missing %s', len(feature_map), len(missed))
    return [feature_map[x] for x in ids if x in feature_map]  # sorted order.


def get_pregrants(ids, cnx, max_query_size=300000):
    # | id | document_number | sequence | name_first | name_last | organization | type | rawlocation_id | city | state | country | filename | created_date | updated_date |
    cursor = cnx.cursor()
    feature_map = dict()
    for idx in range(0, len(ids), max_query_size):
        id_str = ", ".join(['"%s"' % x for x in ids[idx:idx + max_query_size]])
        query = "SELECT id, document_number, name_first, name_last, sequence-1 as sequence, designation, deceased, rawlocation_id, city, state, country, filename, created_date, updated_date FROM rawinventor WHERE id in (%s);" % id_str
        cursor.execute(query)
        for rec in cursor:
            am = InventorMention.from_application_sql_record(rec)
            feature_map[am.uuid] = am
            idx += 1
    missed = [x for x in ids if x not in feature_map]
    logging.warning('[get_pregrants] got: %s, missing %s', len(feature_map), len(missed))
    return [feature_map[x] for x in ids if x in feature_map]  # sorted order.
