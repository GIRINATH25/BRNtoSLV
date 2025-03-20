# import psycopg2
# import pymysql
# import pyodbc
# import cx_Oracle
import clickhouse_connect
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import urllib.parse


def get_engine(config):
    """
    Create an SQLAlchemy engine for various database types.

    Supported databases:
    - PostgreSQL
    - MySQL
    - MSSQL
    - Oracle
    - ClickHouse

    :param config: Dictionary with database connection details.
    :return: SQLAlchemy Engine object configured with the specified connection details.
    :rtype: sqlalchemy.engine.Engine
    """
    dialect = config.get("dialect", "")
    driver = config.get("driver", "")
    conn = config.get("connection", {})

    host = conn.get("host", "")
    port = conn.get("port", "")
    database = conn.get("database", "")
    username = conn.get("username", "")
    password = conn.get("password", "")
    password = urllib.parse.quote_plus(password)

    try:
        # PostgreSQL - psycopg2
        if dialect == "postgresql":
            url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

        # MySQL - PyMySQL
        elif dialect == "mysql":
            url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

        # MSSQL - pyodbc
        elif dialect == "mssql+pyodbc":
            conn_str = f"DRIVER={{{driver}}};SERVER={host};DATABASE={database};UID={username};PWD={password}"
            encoded_conn_str = urllib.parse.quote_plus(conn_str)
            url = f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}"

        # Oracle - cx_Oracle
        elif dialect == "oracle":
            url = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}"

        # ClickHouse - clickhouse_connect
        elif dialect == "clickhouse":
            return clickhouse_connect.get_client(
                                            host=host, 
                                            port=port, 
                                            username=username, 
                                            password=password,
                                            database=database
                                        )

        else:
            print(f"Unsupported database dialect: {dialect}")
            return None

        # Create SQLAlchemy engine with connection pooling
        engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=5
        )
        print(f"SQLAlchemy Engine created for {dialect}")
        return engine

    except Exception as e:
        print(f"engine failed for {dialect}: {str(e)}")
        return None
