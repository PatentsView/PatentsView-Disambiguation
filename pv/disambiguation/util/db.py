
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