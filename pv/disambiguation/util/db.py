
import mysql.connector
from absl import logging

def granted_table(config):
    if config['DISAMBIGUATION']['granted_patent_database'].lower() == 'none':
        logging.info('[granted_table] no db given')
        return None
    else:
        logging.info('[granted_table] trying to connect to %s', config['DISAMBIGUATION']['granted_patent_database'])
        return mysql.connector.connect(host=config['DATABASE']['host'],
                                                 user=config['DATABASE']['username'],
                                                 password=config['DATABASE']['password'],
                                                 database=config['DISAMBIGUATION']['granted_patent_database'])

def pregranted_table(config):
    if config['DISAMBIGUATION']['pregrant_database'].lower() == 'none':
        logging.info('[pregranted_table] no db given')
        return None
    else:
        logging.info('[granted_table] trying to connect to %s', config['DISAMBIGUATION']['pregrant_database'])
        return mysql.connector.connect(host=config['DATABASE']['host'],
                                                 user=config['DATABASE']['username'],
                                                 password=config['DATABASE']['password'],
                                                 database=config['DISAMBIGUATION']['pregrant_database'])

def incremental_pregranted_table(config):
    if config['DISAMBIGUATION']['incremental_pregrant_database'].lower() == 'none':
        logging.info('[incremental_pregranted_table] no db given')
        return None
    else:
        logging.info('[incremental_pregranted_table] trying to connect to %s', config['DISAMBIGUATION']['incremental_pregrant_database'])
        return mysql.connector.connect(host=config['DATABASE']['host'],
                                                 user=config['DATABASE']['username'],
                                                 password=config['DATABASE']['password'],
                                                 database=config['DISAMBIGUATION']['incremental_pregrant_database'])

def incremental_granted_table(config):
    if config['DISAMBIGUATION']['incremental_granted_patent_database'].lower() == 'none':
        logging.info('[incremental_granted_patent_database] no db given')
        return None
    else:
        logging.info('[incremental_granted_patent_database] trying to connect to %s', config['DISAMBIGUATION']['incremental_granted_patent_database'])
        return mysql.connector.connect(host=config['DATABASE']['host'],
                                                 user=config['DATABASE']['username'],
                                                 password=config['DATABASE']['password'],
                                                 database=config['DISAMBIGUATION']['incremental_granted_patent_database'])