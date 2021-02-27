import psycopg2
from config import get_config
from contextlib import contextmanager
from sql_queries import create_table_queries, drop_table_queries

@contextmanager
def connect(database):
    conn = None
    try:
        config = get_config('database.ini', 'postgresql')
        conn = psycopg2.connect(**config, database=database)
        cursor = conn.cursor()
        conn.autocommit = True

        yield cursor

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        conn.close()

def create_database(cursor, database):
    cursor.execute(f"DROP DATABASE IF EXISTS {database};")
    cursor.execute(f"CREATE DATABASE {database} WITH ENCODING 'utf8' TEMPLATE template0;")

def drop_tables(cursor):
    for query in drop_table_queries:
        cursor.execute(query)

def create_tables(cursor):
    for query in create_table_queries:
        cursor.execute(query)

def main():
    new_database = "sparkifydb"

    with connect("postgres") as cursor:
        create_database(cursor, new_database)

    with connect(new_database) as cursor:
        drop_tables(cursor)
        create_tables(cursor)

if __name__ == "__main__":
    main()
