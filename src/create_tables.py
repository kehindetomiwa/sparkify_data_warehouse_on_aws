from config import config
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from utils import connect_to_database


def drop_tables(cur, conn):
    '''
    drops all table listed in <drop_table_queries>
    :param cur: database cursor
    :param conn: database connection
    :return: None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    creates tables listed in <create_table_queries>
    :param cur: database cursor
    :param conn: database connection
    :return: None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    conn = connect_to_database()
    cur = conn.cursor()

    print('dropping all tables ...')
    drop_tables(cur, conn)
    print('tables dropped successfully \n creating tables ... ')
    create_tables(cur, conn)
    print('table created successfully')
    conn.close()
    print('connection closed')


if __name__ == "__main__":
    main()