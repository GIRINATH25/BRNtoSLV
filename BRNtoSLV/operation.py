import sys
from common.logs import logger
from db.db_connector import DBConnector
from common.utils import auditable,time_this
from sqlalchemy import text

engineObj = DBConnector()

class BRNtoSLV:
    def __init__(self,records):
        self.record = records

    @time_this
    @auditable
    def stg_to_dwh(self):
        # print(self.records)
        engine = engineObj.get_engine('staging')
        
        query = f"CALL {self.record.targetschemaname}.{self.record.targetprocedurename}('{self.record.sourceid}', '{self.record.dataflowflag}', '{self.record.targetobject}', 0, 0, 0, 0, NULL, NULL);"
        logger.info(query)
        
        with engine.connect() as conn:
            res = conn.execute(text(query))
            source_count, insert_count, update_count, delete_count, flag1, flag2 = res.fetchone()
            conn.commit()
        
        print(source_count," ",update_count," ",insert_count)
        # source_count, insert_count, update_count = (1,2,3)
        # if(source_count and insert_count and update_count):
        #     print(f"empty")
        #     sys.exit(0)
        # print(f"source_count: {source_count}, insert_count: {insert_count}, update_count: {update_count}")
        return (source_count, insert_count, update_count)