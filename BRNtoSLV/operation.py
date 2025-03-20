import re
import sys
from common.logs import logger
from db.db_connector import DBConnector
from common.utils import auditable,time_this
from sqlalchemy import text
from BRNtoSLV.table_generation_SLV import generate_create_table_sql, detect_database_type
from BRNtoSLV.postgres_sp_generation import generate_stored_procedure

engineObj = DBConnector()
def create_table_SLV(engine,record):
    with engine.connect() as conn:
                param = {'target_table':record.targetobject}
                res = conn.execute(text('''
                                            SELECT COUNT(*) AS cn
                                            FROM INFORMATION_SCHEMA.TABLES
                                            WHERE TABLE_NAME = :target_table
                                            '''),param)
                res = res.fetchone()
                conn.commit()

    if res[0] == 0:
                logger.info(f'target table unavailable => Creating it {record.targetobject}')
                create_script = generate_create_table_sql(engine, record.sourceobject, record.targetobject)
                db_type = detect_database_type(engine)

                if db_type == 'mysql':
                    with engine.connect() as conn:
                        conn.execute(text(create_script))
                        conn.commit()
                else:
                    schema_available(engine,record)
                    create_script = add_schema_to_create_table(create_script,record.targetschemaname)
                    with engine.connect() as conn:
                        conn.execute(text(create_script))
                        conn.commit()


def schema_available(engine,record):
    with engine.connect() as conn:
        param = {'schema':record.targetschemaname}
        res = conn.execute(text('''
                                    SELECT COUNT(SCHEMA_NAME)
                                    FROM INFORMATION_SCHEMA.SCHEMATA
                                    WHERE SCHEMA_NAME = :schema '''),param)
        res = res.fetchall()
        conn.commit()

    if res[0] == 0: 
        with engine.connect() as conn:
            logger.info(f'target schema unavailable => Creating it {record.targetschemaname}')
            param = {'targetschemaname':record.targetschemaname}
            conn.execute(text('CREATE SCHEMA :targetschemaname'),param)
            conn.commit()

def add_schema_to_create_table(create_script, target_schemaname):
    """
    Modify CREATE TABLE statement to add schema before table name.
    """
    pattern = r"CREATE TABLE\s+(\w+)\s*\(" 
    replacement = f"CREATE TABLE {target_schemaname}.\\1 ("  

    modified_script = re.sub(pattern, replacement, create_script, count=1)  
    return modified_script


def create_sp_generation(engine,record):
    with engine.connect() as conn:
        res = conn.execute(text(f'''
                                    SELECT COUNT(SPECIFIC_NAME)
                                    FROM INFORMATION_SCHEMA.ROUTINES
                                    WHERE ROUTINE_NAME = '{record.targetprocedurename}' '''))
        res = res.fetchall()
        conn.commit()
    
    if res[0][0] == 0:
        with engine.connect() as conn:
            logger.info(f'target procedure unavailable => Creating it {record.targetprocedurename}')
            query = generate_stored_procedure(engine,record.targetschemaname,record.targetobject,'stg',record.sourceobject)
            res = conn.execute(text(query))
            conn.commit()


class BRNtoSLV:
    def __init__(self,records):
        self.record = records
    
    @time_this
    @auditable
    def stg_to_dwh(self):
        # print(self.records)
        engine = engineObj.get_engine('staging')

        create_table_SLV(engine,self.record)
        create_sp_generation(engine,self.record)
        
        query = f"CALL {self.record.targetschemaname}.{self.record.targetprocedurename}('{self.record.sourceid}', '{self.record.dataflowflag}', '{self.record.targetobject}', 0, 0, 0, 0, NULL, NULL);"
        logger.info(query)
        
        with engine.connect() as conn:
            res = conn.execute(text(query))
            source_count, insert_count, update_count, delete_count, flag1, flag2 = res.fetchone()
            conn.commit()
        
        return (source_count, insert_count, update_count)

