
import mysql.connector

def granted_table(config):
    return mysql.connector.connect(host=config['DATABASE']['host'],
                                             user=config['DATABASE']['username'],
                                             password=config['DATABASE']['password'],
                                             database=config['DATABASE']['granted_table'])

def pregranted_table(config):
    return mysql.connector.connect(host=config['DATABASE']['host'],
                                             user=config['DATABASE']['username'],
                                             password=config['DATABASE']['password'],
                                             database=config['DATABASE']['pregranted_table'])