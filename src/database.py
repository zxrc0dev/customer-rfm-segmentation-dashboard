import re
from sqlalchemy import text
from sqlalchemy.engine import URL


def create_database(engine: URL, db_name: str):
    if not re.match(r'^[a-zA-Z0-9_]+$', db_name):
        raise ValueError("Invalid database name.")

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:

        terminate_query = f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{db_name}'
          AND pid <> pg_backend_pid();
        """
        conn.execute(text(terminate_query))

        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name};"))
        conn.execute(text(f"CREATE DATABASE {db_name} ENCODING='UTF8';"))

    print(f"Database '{db_name}' created successfully.")


def create_table(engine, table_name: str):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS public.{table_name} (
        invoice      varchar(50) NOT NULL,
        stock_code    varchar(50),
        description  text,
        quantity     integer,
        invoice_date  timestamp without time zone,
        price        numeric(10,2),
        customer_id  integer,
        country      varchar(100),
        revenue      numeric(20,2),
        year         integer,
        month        integer
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        print(f"Table '{table_name}' created successfully in schema 'public'.")

def load_data(data_path: str, engine, table_name: str):

    copy_sql = f"""
    COPY public.{table_name}(invoice,stock_code,description,quantity,invoice_date,price,customer_id,country,revenue,year,month)
    FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',');
    """
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            with open(data_path, 'r') as f:
                cursor.copy_expert(copy_sql, f)
        raw_conn.commit()
        print(f"Data loaded successfully into table '{table_name}'.")
    finally:
        raw_conn.close()