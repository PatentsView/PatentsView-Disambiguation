import os
import pickle

import mysql.connector
from absl import logging

from pv.disambiguation.core import InventorMention


class Loader(object):
    def __init__(self, pregranted_canopies, granted_canopies):
        self.pregranted_canopies = pregranted_canopies
        self.granted_canopies = granted_canopies
        self.cnx_g = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                             database='patent_20200630')
        self.cnx_pg = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'], '.mylogin.cnf'),
                                              database='pregrant_publications')

    def load(self, canopy):
        return load_canopy(canopy,
                           self.pregranted_canopies[canopy] if canopy in self.pregranted_canopies else [],
                           self.granted_canopies[canopy] if canopy in self.granted_canopies else [],
                           self.cnx_pg, self.cnx_g)

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
                           self.cnx_pg, self.cnx_g)

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


def load_canopy(canopy_name, pregrant_ids, granted_ids, cnx_pg, cnx_g):
    logging.info('Loading data from canopy %s, %s pregranted, %s granted', canopy_name, len(pregrant_ids),
                 len(granted_ids))
    rec = (get_pregrants(pregrant_ids, cnx_pg) if pregrant_ids else []) + (
        get_granted(granted_ids, cnx_g) if granted_ids else [])
    return rec


def get_granted(ids, cnx, max_query_size=300000):
    # | uuid | patent_id | assignee_id | rawlocation_id | type | name_first | name_last | organization | sequence |
    # cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'],'.mylogin.cnf'), database='patent_20200630')
    cursor = cnx.cursor()
    feature_map = dict()
    for idx in range(0, len(ids), max_query_size):
        id_str = ", ".join(['"%s"' % x for x in ids[idx:idx + max_query_size]])
        query = "SELECT * FROM rawinventor WHERE uuid in (%s);" % id_str
        cursor.execute(query)
        for rec in cursor:
            am = InventorMention.from_granted_sql_record(rec)
            feature_map[am.uuid] = am
    missed = [x for x in ids if x not in feature_map]
    logging.warning('[get_granted] missing %s ids: %s', len(missed), str(missed))
    return [feature_map[x] for x in ids if x in feature_map]  # sorted order.


def get_pregrants(ids, cnx, max_query_size=300000):
    # | id | document_number | sequence | name_first | name_last | organization | type | rawlocation_id | city | state | country | filename | created_date | updated_date |
    # cnx = mysql.connector.connect(option_files=os.path.join(os.environ['HOME'],'.mylogin.cnf'), database='pregrant_publications')
    cursor = cnx.cursor()
    feature_map = dict()
    for idx in range(0, len(ids), max_query_size):
        id_str = ", ".join(['"%s"' % x for x in ids[idx:idx + max_query_size]])
        query = "SELECT * FROM rawinventor WHERE id in (%s);" % id_str
        cursor.execute(query)
        for rec in cursor:
            am = InventorMention.from_application_sql_record(rec)
            feature_map[am.uuid] = am
            idx += 1
    missed = [x for x in ids if x not in feature_map]
    logging.warning('[get_pregrants] missing %s ids: %s', len(missed), str(missed))
    return [feature_map[x] for x in ids if x in feature_map]  # sorted order.
