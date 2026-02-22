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
        stockcode    varchar(50),
        description  text,
        quantity     integer,
        invoicedate  timestamp without time zone,
        price        numeric(10,2),
        customer_id  integer,
        country      varchar(100)
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        print(f"Table '{table_name}' created successfully in schema 'public'.")

def load_data(data_path: str, engine, table_name: str):
    copy_sql = f"""
    COPY public.{table_name}(invoice, stockcode, description, quantity, invoicedate, price, customer_id, country)
    FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',');
    """
    with engine.connect() as conn:
        with open(data_path, 'r') as f:
            conn.connection.cursor().copy_expert(copy_sql, f)
        print(f"Data loaded successfully into table '{table_name}'.")