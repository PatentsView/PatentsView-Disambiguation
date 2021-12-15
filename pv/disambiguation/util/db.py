import mysql.connector
from absl import logging


def connect_to_disambiguation_database(config, dbtype='granted_patent_database'):
    if config['DISAMBIGUATION'][dbtype].lower() == 'none':
        logging.info('{dbtype} no db given'.format(dbtype=dbtype))
        return None
    else:
        logging.info('[{%s}] trying to connect to %s', dbtype,
                     config['DISAMBIGUATION']['granted_patent_database'])
        return mysql.connector.connect(host=config['DATABASE_SETUP']['host'],
                                       user=config['DATABASE_SETUP']['username'],
                                       password=config['DATABASE_SETUP']['password'],
                                       database=config['DISAMBIGUATION'][dbtype],
                                       charset='utf8mb4',
                                       collation='utf8mb4_unicode_ci')


def granted_table(config):
    if config['DISAMBIGUATION']['granted_patent_database'].lower() == 'none':
        logging.info('[granted_table] no db given')
        return None
    else:
        logging.info('[granted_table] trying to connect to %s', config['DISAMBIGUATION']['granted_patent_database'])
        return mysql.connector.connect(host=config['DATABASE_SETUP']['host'],
                                       user=config['DATABASE_SETUP']['username'],
                                       password=config['DATABASE_SETUP']['password'],
                                       database=config['DISAMBIGUATION']['granted_patent_database'])


def pregranted_table(config):
    if config['DISAMBIGUATION']['pregrant_database'].lower() == 'none':
        logging.info('[pregranted_table] no db given')
        return None
    else:
        logging.info('[granted_table] trying to connect to %s', config['DISAMBIGUATION']['pregrant_database'])
        return mysql.connector.connect(host=config['DATABASE_SETUP']['host'],
                                       user=config['DATABASE_SETUP']['username'],
                                       password=config['DATABASE_SETUP']['password'],
                                       database=config['DISAMBIGUATION']['pregrant_database'])


def incremental_pregranted_table(config):
    if config['DISAMBIGUATION']['incremental_pregrant_database'].lower() == 'none':
        logging.info('[incremental_pregranted_table] no db given')
        return None
    else:
        logging.info('[incremental_pregranted_table] trying to connect to %s',
                     config['DISAMBIGUATION']['incremental_pregrant_database'])
        return mysql.connector.connect(host=config['DATABASE_SETUP']['host'],
                                       user=config['DATABASE_SETUP']['username'],
                                       password=config['DATABASE_SETUP']['password'],
                                       database=config['DISAMBIGUATION']['incremental_pregrant_database'])


def incremental_granted_table(config):
    if config['DISAMBIGUATION']['incremental_granted_patent_database'].lower() == 'none':
        logging.info('[incremental_granted_patent_database] no db given')
        return None
    else:
        logging.info('[incremental_granted_patent_database] trying to connect to %s',
                     config['DISAMBIGUATION']['incremental_granted_patent_database'])
        return mysql.connector.connect(host=config['DATABASE_SETUP']['host'],
                                       user=config['DATABASE_SETUP']['username'],
                                       password=config['DATABASE_SETUP']['password'],
                                       database=config['DISAMBIGUATION']['incremental_granted_patent_database'])
