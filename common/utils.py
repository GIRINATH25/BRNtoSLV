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
from common.postgres_sp_generation import generate_stored_procedure
from db.db_connector import DBConnector
import db.postgres_query as q
from common.table_generation_SLV import generate_create_table_sql, detect_database_type

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
                source_count += 1
            elif not isinstance(i, tuple) and hasattr(i, "__class__"):
                classObj = i

        if classObj is not None and nametup is None:
            record = classObj.record
        else:
            record = nametup
        
        
        # print(dir(record))
        
        try:
            engine = db.get_engine('staging')
            
            if record.dataflowflag == 'BRNtoSLV':
                create_table_SLV(engine,record)
                create_sp_generation(engine,record)
            
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
        result = conn.execute(text("CALL ods.usp_etlpreprocess( :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :source_count, :user_agent, :etl_batch_id, NULL)"),params)
        conn.commit()
        latestbatchid_row = result.fetchone()
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
        conn.execute(text("CALL ods.usp_etlpostprocess( :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :latestbatchid, :source_count, :insert_count, :update_count)"), params)
        conn.commit()


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
        conn.execute(text("CALL ods.usp_etlerrorinsert( :sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line )"), params)


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
