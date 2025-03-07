from db.model import create_all
from db.db_connector import DBConnector
from sqlalchemy import text
import db.postgres_query as q

db = DBConnector()

def main():
    engine = db.get_engine('source_config')

    # This will create controlheader,controldetail,audit,error tables with schema "ods"
    create_all(engine)

    # check for preprocess etl sp if not it will create it(usp_etlpreprocess)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_preprocess))
        res = res.fetchone()
        conn.commit()
        
    
    if (not res):
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_etlpreprocess))
            conn.commit()

    # check for postprocess etl sp if not it will create it(usp_etlpostprocess)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_postprocess))
        res = res.fetchone()
        conn.commit()
    
    if (not res):
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_etlpostprocess))
            conn.commit()

    # check for errorinsert etl sp if not it will create it(usp_etlerrorinsert)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_errorinsert))
        res = res.fetchone()
        conn.commit()
    
    if (not res):
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_etlerrorinsert))
            conn.commit()


if __name__ == '__main__':
    main()