from db.db_connector import DBConnector
from sqlalchemy import text



db = DBConnector()

for _ in range(0,10):
    source = db.source()
    staging = db.staging()


    with source.connect() as conn:
        res = conn.execute(text("select 'hello' AS SQLSERVER"))
        print(res.fetchall())
        conn.commit()

    with staging.connect() as conn:
        res = conn.execute(text("SELECT 'hello' AS POSTGRESQL"))
        print(res.fetchall())
        conn.commit()