import psycopg2
import sys
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    # TODO: Any sensitive/confidential details must not be in plain-text. In production environment
    # these database credentials must be stored securely and fetched securely.
    try :
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        print(e)
        sys.exit("Exiting program : Authentication Failed")
    except psycopg2.Error as e:
        print(e)
        sys.exit("Error: Exiting program")
    
    # create sparkify database with UTF8 encoding
    try :
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.ProgrammingError as e:
        print(e)
        print("Warning: Database already exists ")
    except psycopg2.Error as e :
        print(e)
        sys.exit("Error: Could not create database. Exiting ...")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    # TODO: Any sensitive/confidential details must not be in plain-text. In production environment
    # these Database credentials must be stored securely and fetched securely.
    try :
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        print(e)
        sys.exit("Exiting program : Authentication Failed")
    except psycopg2.Error as e:
        print(e)
        sys.exit("Error: Exiting program")
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: issue dropping table")
            print(e)


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: issue creating table")
            print(e)
            



def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()