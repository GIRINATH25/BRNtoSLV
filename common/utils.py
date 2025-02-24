"""

Need fix for user_agent, etl_batch_id, sourcegroupflag

"""

import functools
import time
import logging
from sqlalchemy import text
from common.logs import logger
from common import logs
import random
import string
from db.db_connector import DBConnector

db = DBConnector()

def time_this(function):
    """
        To find execution time of a function.
        -------------------------------------
    """
    @functools.wraps(function)
    def wrapper(record, *args, **kwargs):
        time_start = time.perf_counter()
        result = function(record, *args, **kwargs)
        logger.info(
            f'{record["sourceid"].ljust(35)}{str(result[0]).rjust(8)} rows\t'
            f'{time.perf_counter() - time_start:5.2f} seconds\t'
            f'{record["targetobject"].ljust(39)}\t'
        )
        return result
    return wrapper

def findmodule(dataflowflag):
    """
    Find and return which module to use based on dataflowflag.
    ---------------------------------------------------------
    Keyword arguments:
    flag from controldetail and controlheader
    Return: string of module name
    """
    
    dataflowflag = dataflowflag[:3].lower()
    modules = {
            'src': 'sourcetostaging',
            'stg': 'stagingtodwh',
        }
    return modules.get(dataflowflag, 'dwhtoclick')

def auditable(function):
    @functools.wraps(function)
    def wrapper(record, *args, **kwargs):
        try:
            user_agent = 'Python'
            etl_batch_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(11))
            latestbatchid = audit_start(
                record["sourceid"],
                record["targetobject"],
                record["dataflowflag"],
                args and len(args[0]) or 0,
                user_agent,
                etl_batch_id
            )
            record = record._replace(latestbatchid=latestbatchid)
            source_count, insert_count, update_count = function(record, *args, **kwargs)
            
            audit_end(
                record["sourceid"],
                record["targetobject"],
                record["dataflowflag"],
                record["latestbatchid"],
                source_count,
                insert_count,
                update_count,
            )
            return source_count, insert_count, update_count
        except Exception as exc:
            err_info = logs.error_info(exc)
            logger.info('{tb}'.format(**err_info))
            logs.log_error(
                err_info,
                src=f'{function.__module__}.{function.__name__}',
                obj_type=record["objecttype"],
                sourceid=record["sourceid"],
                target_file=record["targetobject"],
                latestbatchid=record["latestbatchid"],
            )

            audit_error(
                record["sourceid"],
                record["targetobject"],
                record["dataflowflag"],
                record["latestbatchid"],
                function.__name__,
                f'{findmodule(record["dataflowflag"])}.{function.__module__}',
                -1,
                '{type}: {args}'.format(**err_info),
                exc.__traceback__.tb_lineno,
            )
            
            logs.save()
    return wrapper


def audit_start(sourceid, targetobject, dataflowflag, source_count, user_agent, etl_batch_id):

    query = "EXEC dbo.usp_etlpreprocess :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :source_count, :user_agent, :etl_batch_id, NULL"
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'sourcegroupflag': 1 if 1 else 0, 
        'source_count': source_count, 
        'user_agent': user_agent, 
        'etl_batch_id': etl_batch_id
        }
        
    engine  = db.staging()
    
    with engine.connect() as conn:
        result = conn.execute(text(query),params)
        conn.commit()
        latestbatchid_row = result.fetchone()
        if latestbatchid_row:
            latestbatchid = latestbatchid_row[0]

        return latestbatchid

def audit_end(sourceid, targetobject, dataflowflag, latestbatchid, source_count, insert_count, update_count):

    query = "EXEC dbo.usp_etlpostprocess :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :latestbatchid, :source_count, :insert_count, :update_count"
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'sourcegroupflag': 1 if 1 else 0, 
        'latestbatchid': latestbatchid, 
        'source_count': source_count, 
        'insert_count': insert_count, 
        'update_count': update_count
        }
    
    engine = db.staging()

    with engine.connect() as conn:
        conn.execute(text(query), params)
        conn.commit()


@logs.handle_error
def audit_error(sourceid, targetobject, dataflowflag, latestbatchid, task, package, error_id, error_desc, error_line):

    query = "EXEC dbo.usp_etlerrorinsert :sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line"
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'latestbatchid': latestbatchid, 
        'task': task, 
        'package': package, 
        'error_id': error_id, 
        'error_desc': error_desc, 
        'error_line': error_line
        }
    
    engine = db.staging()
    with engine.connect() as conn:
        conn.execute(text(query), params)
