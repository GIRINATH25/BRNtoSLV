from db.db_connector import DBConnector
from sqlalchemy import text

db = DBConnector()

engine = db.get_engine('source')


with engine.connect() as con:
    res = con.execute(text("SELECT 'hello' as new"))
    print(res.fetchall())

print(engine)