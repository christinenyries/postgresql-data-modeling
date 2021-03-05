import psycopg2
from config import get_config
from contextlib import contextmanager
from sql_queries import create_table_queries, drop_table_queries

def main():
    default_database = "postgres"
    new_database = "sparkifydb"

    with connect(default_database) as cursor:
        create_database(cursor, new_database)

    with connect(new_database) as cursor:
        drop_tables(cursor)
        create_tables(cursor)

@contextmanager
def connect(database):
    conn = None
    try:
        print(f"Attempting to connect to '{database}'...")
        config = get_config('database.ini', 'postgresql')
        conn = psycopg2.connect(**config, database=database)
        print(f"Successfully connected to '{database}'...")

        cursor = conn.cursor()
        conn.autocommit = True

        yield cursor

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        print(f"Closing connection to '{database}'...")
        conn.close()

def create_database(cursor, database):
    cursor.execute(f"DROP DATABASE IF EXISTS {database};")
    cursor.execute(f"CREATE DATABASE {database} WITH ENCODING 'utf8' TEMPLATE template0;")
    print(f"Created '{database}'...")

def drop_tables(cursor):
    for query in drop_table_queries:
        cursor.execute(query)
    print(f"Dropped existing tables...")

def create_tables(cursor):
    for query in create_table_queries:
        cursor.execute(query)
    print(f"Done creating tables...")

if __name__ == "__main__":
    main()
