import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, schema



def set_schema(cur, conn):
    '''
    Executes queries to setup database schema
            Parameters:
                    a cursor
                    b database connection string
    '''
    print(schema)
    cur.execute(schema)
    print(" ***** Distribution schema created succesfully. ***** ")

def drop_tables(cur, conn):
    '''
    Executes queries to drop any existing tables
            Parameters:
                    a cursor
                    b database connection string
    '''     
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print(" ***** Tables dropped succesfully. ***** ")

def create_tables(cur, conn):
    '''
    Executes queries to create staging and star schema tables
            Parameters:
                    a cursor
                    b database connection string
    ''' 
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print(" ***** Tables created succesfully. ***** ")


def main():
    '''
    Establishes a connection to Redshift Cluster using dwh.config and executes Table setup queries
    ''' 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    set_schema (cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)
    

    conn.close()


if __name__ == "__main__":
    main()