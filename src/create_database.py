import re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

def create_database(base_url: URL, db_name: str):
    if not re.match(r'^[a-zA-Z0-9_]+$', db_name):
        raise ValueError("Invalid database name.")
    
    engine = create_engine(base_url.set(database="postgres"))

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
