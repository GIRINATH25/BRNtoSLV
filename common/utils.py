"""

Need fix for user_agent, sourcegroupflag

"""
import re
import functools
import time
from sqlalchemy import text
from common.logs import logger
from common import logs
import random
import string
from db.db_connector import DBConnector
import db.postgres_query as q

db = DBConnector()

def time_this(function):
    """
        To find execution time of a function.(Use single record from controlheader and controldetail)
        --------------------------------------------------------------------------------------------
    """
    @functools.wraps(function)
    def wrapper(*args):
        record, classObj, nametup = None, None, None

        for i in args:
            if isinstance(i, tuple) and hasattr(i, "_fields"):
                nametup = i
            elif not isinstance(i, tuple) and hasattr(i, "__class__"):
                classObj = i

        if classObj is not None and nametup is None:
            record = classObj.record
        else:
            record = nametup

        # print(record)

        time_start = time.perf_counter()
        result = function(*args)
        logger.info(
            f'source => {record.sourceid} {str(result[0]).rjust(8)} rows\t'
            f'{time.perf_counter() - time_start:5.2f} seconds\t'
            f'target => {record.targetobject}\t'
        )
        return result
    return wrapper

def total_time_this(function):    
    """
        To find execution total time taken of a function.
        ------------------------------------------------
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        time_start = time.perf_counter()
        result = function(*args, **kwargs)
        logger.info(f'Total time taken: {time.perf_counter() - time_start:5.2f} seconds')
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
            'src': 'SRCtoBRN',
            'brn': 'BRNtoSLV',
        }
    return modules.get(dataflowflag, 'dwhtoclick')

def auditable(function):
    @functools.wraps(function)
    def wrapper(*args):
        record, classObj, nametup = None, None, None
        source_count = 0
        # print(args)
        for i in args:
            if isinstance(i, tuple) and hasattr(i, "_fields"):
                nametup = i
                # source_count += 1
            elif not isinstance(i, tuple) and hasattr(i, "__class__"):
                classObj = i

        if classObj is not None and nametup is None:
            record = classObj.record
        else:
            record = nametup
        
        
        # print(dir(record))
        
        try:
            engine = db.get_engine('staging')
            
            
            user_agent = 'Python'
            etl_batch_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(11))
            latestbatchid = audit_start(
                record.sourceid,
                record.targetobject,
                record.dataflowflag,
                source_count,
                user_agent,
                etl_batch_id,
                engine
            )
            record = record._replace(latestbatchid=latestbatchid)
            # print(args)
            source_count, insert_count, update_count = function(*args)
            
            audit_end(
                record.sourceid,
                record.targetobject,
                record.dataflowflag,
                record.latestbatchid,
                source_count,
                insert_count,
                update_count,
                engine
            )
            return source_count, insert_count, update_count
        except Exception as exc:
            err_info = logs.error_info(exc)
            logger.info('{tb}'.format(**err_info))
            logs.log_error(
                err_info,
                src=f'{function.__module__}.{function.__name__}',
                obj_type=record.objecttype,
                sourceid=record.sourceid,
                target_file=record.targetobject,
                latestbatchid=record.latestbatchid,
            )

            audit_error(
                record.sourceid,
                record.targetobject,
                record.dataflowflag,
                record.latestbatchid,
                function.__name__,
                f'{findmodule(record.dataflowflag)}.{function.__module__}',
                -1,
                '{type}: {args}'.format(**err_info),
                exc.__traceback__.tb_lineno,
                engine
            )
            
            logs.save()
    return wrapper


def audit_start(sourceid, targetobject, dataflowflag, source_count, user_agent, etl_batch_id,engine):
    
    params = {
        'sourceid': sourceid, 
        'targetobject': targetobject, 
        'dataflowflag': dataflowflag, 
        'sourcegroupflag': 1 if 1 else 0, 
        'source_count': source_count, 
        'user_agent': user_agent, 
        'etl_batch_id': etl_batch_id
        }
        
    
    with engine.connect() as conn:
        with conn.begin():
            result = conn.execute(text("CALL ods.usp_etlpreprocess( :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :source_count, :user_agent, :etl_batch_id, NULL)"),params)
            latestbatchid_row = result.fetchone()
            # conn.commit()
        if latestbatchid_row:
            latestbatchid = latestbatchid_row[0]
        
        return latestbatchid

def audit_end(sourceid, targetobject, dataflowflag, latestbatchid, source_count, insert_count, update_count,engine):

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

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("CALL ods.usp_etlpostprocess( :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :latestbatchid, :source_count, :insert_count, :update_count)"), params)
            # conn.commit()


@logs.handle_error
def audit_error(sourceid, targetobject, dataflowflag, latestbatchid, task, package, error_id, error_desc, error_line, engine):

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
    
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("CALL ods.usp_etlerrorinsert( :sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line )"), params)


