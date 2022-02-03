import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load the staging_events and staging_songs data from S3 bucket into Redshift staging tables:
    staging_events_copy and staging_songs_copy
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data in staging tables to fact table songplays and dimention tables: users, songs, artists and time
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The ETL process
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()