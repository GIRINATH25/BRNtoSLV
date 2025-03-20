from sqlalchemy import text
from db.db_connector import DBConnector


db = DBConnector()

engine = db.get_engine("staging")

print(engine)
with engine.connect() as conn:
    print(conn)
    result = conn.execute(text("SELECT 'connected' as test"))
    result = result.fetchall()
    for row in result:
        print(row) 