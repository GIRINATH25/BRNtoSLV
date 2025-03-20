import time
from db.model import create_all
from db.db_connector import DBConnector
from sqlalchemy import text
import db.postgres_query as q

db = DBConnector()

def main():
    engine = db.get_engine('source_config')

    # This will create controlheader,controldetail,audit,error tables with schema "ods"
    print('creating needed tables for ods schema')
    create_all(engine)

    # check for preprocess etl sp if not it will create it(usp_etlpreprocess)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_preprocess))
        res = res.fetchone()
        conn.commit()
        
    
    if (not res):
        print('creating usp_etlpreprocess')
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_etlpreprocess))
            conn.commit()

    # check for postprocess etl sp if not it will create it(usp_etlpostprocess)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_postprocess))
        res = res.fetchone()
        conn.commit()
    
    if (not res):
        print('creating usp_etlpostprocess')
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_etlpostprocess))
            conn.commit()

    # check for errorinsert etl sp if not it will create it(usp_etlerrorinsert)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_errorinsert))
        res = res.fetchone()
        conn.commit()
    
    if (not res):
        print('creating usp_etlerrorinsert')
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_etlerrorinsert))
            conn.commit()

    # check for lookup etl sp if not it will create it(usp_etllookup)
    with engine.connect() as conn:
        res = conn.execute(text(q.find_sp_usp_SrcToMain_lookup))
        res = res.fetchone()
        conn.commit()

    if (not res):
        print('creating usp_etllookup')
        with engine.connect() as conn:
            res = conn.execute(text(q.postgres_sp_SRCtoMAIN_lookup))
            conn.commit()
            time.sleep(5)

    with engine.connect() as conn:
        print('Inserting data into datatype_mapping')
        res = conn.execute(text(
            "INSERT INTO ods.datatype_mapping (sourcedatabasetype, targetdatabasetype, sourcedatatypes, targetdatatypes) VALUES " +
                ", ".join(["(:src_db, :tgt_db, :src_type, :tgt_type)"] * len(q.data))
        ),
        [{"src_db": src_db, "tgt_db": tgt_db, "src_type": src_type, "tgt_type": tgt_type} for src_db, tgt_db, src_type, tgt_type in q.data]
        )
        conn.commit()

    with engine.connect() as conn:
        print('clickhouse table creation sp')
        res = conn.execute(text(q.golden_layer_sp))
        conn.commit()


if __name__ == '__main__':
    main()