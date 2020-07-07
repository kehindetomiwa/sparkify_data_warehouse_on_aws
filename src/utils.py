from config import config
import psycopg2


print(config['CLUSTER']['HOST'])

def connect_to_database():
    '''
    establish connection to redshift database
    :return:
    '''
    HOST = config['CLUSTER']['HOST']
    DB_NAME = config['CLUSTER']['DB_NAME']
    DB_USER = config['CLUSTER']['DB_USER']
    DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
    DB_PORT = config['CLUSTER']['DB_PORT']

    CONNECT_STRING = "host={} user={} password={} port={}".format(
        HOST,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
        DB_PORT
    )
    print('Connection to Redshift', CONNECT_STRING)
    conn = psycopg2.connect(CONNECT_STRING)
    print('connected to Redshift')
    return conn


