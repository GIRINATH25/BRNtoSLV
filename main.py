from db.db_connector import DBConnector
from sqlalchemy import text
from dub import dub


db = DBConnector()

for _ in range(0,10):
    source = db.source()
    staging = db.staging()
    source_config = db.source_config()

    dub()

    with source.connect() as conn:
        res = conn.execute(text("select 'hello source main' AS SQLSERVER"))
        print(res.fetchall())
        conn.commit()

    with staging.connect() as conn:
        res = conn.execute(text("SELECT 'hello staging main' AS POSTGRESQL"))
        print(res.fetchall())
        conn.commit()

    with source_config.connect() as conn:
        res = conn.execute(text("SELECT 'hello source_config main' AS POSTGRESQL"))
        print(res.fetchall())
        conn.commit()