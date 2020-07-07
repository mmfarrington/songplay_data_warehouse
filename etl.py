import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, test_queries

def load_staging_tables(cur, conn):
    '''
    Executes queries to copy data from S3 files into Redshift staging tables
            Parameters:
                    a cursor
                    b database connection string
    ''' 
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print(" ***** Data copied from S3 succesfully. ***** ")

def insert_tables(cur, conn):
    '''
    Executes queries to insert data from staging tables into fact/dimension tables
            Parameters:
                    a cursor
                    b database connection string
    ''' 
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print(" ***** Data inserted into staging tables succesfully. ***** ")

def test (cur, conn): 
    '''
    Executes test queries verify correct setup of fact/dimension tables
            Parameters:
                    a cursor
                    b database connection string
    ''' 
    for query in test_queries:
        cur.execute(query)
        conn.commit()
        row = cur.fetchall()
        if row is not None:
            print(row)
    print(" ***** Test queries completed succesfully. ***** ")

def main():
    '''
    Establishes a connection to Redshift Cluster using dwh.config and executes ETL queries
    
    ''' 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    test(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()