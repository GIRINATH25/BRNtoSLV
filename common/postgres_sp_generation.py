from sqlalchemy import text
import difflib

def detect_database_type(engine):
    """Detect the database type using SQLAlchemy engine dialect."""
    db_type = engine.dialect.name.lower()
    if db_type == "postgresql":
        return "postgresql"
    else:
        raise ValueError(f"Unsupported Database: {db_type}")

def fetch_table_columns(engine, schema_name, table_name):
    """Fetch column names for a given table in PostgreSQL."""
    query = f"""
    SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [row[0] for row in result.fetchall()]

def fetch_primary_unique_columns(engine, schema_name, table_name):
    """
    Fetch primary and unique key columns from a PostgreSQL table.
    """
    query = f"""
    SELECT column_name 
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
    WHERE table_schema = '{schema_name}' 
    AND table_name = '{table_name}';
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [row[0] for row in result.fetchall()]

def find_best_matching_columns(source_columns, target_columns):
    """
    Dynamically find the best matching columns between source and target tables
    using substring matching and similarity scoring.
    """
    matched_columns = {}
    for s_col in source_columns:
        best_match = difflib.get_close_matches(s_col, target_columns, n=1, cutoff=0.6)
        if best_match:
            matched_columns[s_col] = best_match[0]  
    return matched_columns

def generate_stored_procedure(engine, target_schema='dwh', target_table=None, source_schema='stg', source_table=None):
    """Generate a PostgreSQL Stored Procedure dynamically based on table structure."""
    db_type = detect_database_type(engine)
    if db_type != "postgresql":
        raise ValueError("Only PostgreSQL is supported for SP generation.")

    target_columns = fetch_table_columns(engine, target_schema, target_table)
    source_columns = fetch_table_columns(engine, source_schema, source_table)
    key_columns = fetch_primary_unique_columns(engine, source_schema, source_table)

    matched_columns = find_best_matching_columns(source_columns, target_columns)

    filtered_match_columns = {s_col: matched_columns[s_col] for s_col in key_columns if s_col in matched_columns} 

    if not filtered_match_columns:
        match_conditions = "1=1"
        insert_where_condition = "1=1"  
    else:
        match_conditions = " AND ".join([
            f"t.{t_col} = s.{s_col}"
            for s_col, t_col in filtered_match_columns.items()
        ])
        insert_where_condition = f"t.{list(filtered_match_columns.values())[0]} IS NULL"

    update_columns = [col for col in target_columns if col in source_columns]
    update_set_clause = ",\n        ".join([f"{col} = s.{col}" for col in update_columns] + [
        "etlactiveind = 1",
        "etljobname = p_etljobname",
        "envsourcecd = p_envsourcecd",
        "datasourcecd = p_datasourcecd",
        "etlupdateddatetime = NOW()"
    ])

    target_columns = [col for col in target_columns if not col.endswith("key")]
    insert_columns = ", ".join(target_columns)
    insert_values = ", ".join([f"s.{col}" if col in source_columns else f"p_{col}" for col in target_columns])

    sp_template = f"""
CREATE OR REPLACE PROCEDURE {target_schema}.usp_{target_table}(
    IN p_sourceid character varying,
    IN p_dataflowflag character varying,
    IN p_targetobject character varying,
    OUT srccnt integer,
    OUT inscnt integer,
    OUT updcnt integer,
    OUT dltcount integer,
    INOUT flag1 character varying,
    OUT flag2 character varying
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    p_etljobname VARCHAR(100);
    p_envsourcecd VARCHAR(50);
    p_datasourcecd VARCHAR(50);
    p_batchid integer;
    p_taskname VARCHAR(100);
    p_rawstorageflag integer;
    p_packagename  VARCHAR(100);
    p_errorid integer;
    p_errordesc character varying;
    p_errorline integer;
    p_etlactiveind integer DEFAULT 1;
    p_etlcreateddatetime TIMESTAMP DEFAULT NOW();
    p_etlupdateddatetime TIMESTAMP DEFAULT NOW();

BEGIN
    -- Fetch metadata
    SELECT d.jobname, h.envsourcecode, h.datasourcecode, d.latestbatchid, d.targetprocedurename, h.rawstorageflag
    INTO p_etljobname, p_envsourcecd, p_datasourcecd, p_batchid, p_taskname, p_rawstorageflag
    FROM ods.controldetail d
    INNER JOIN ods.controlheader h
        ON d.sourceid = h.sourceid
    WHERE d.sourceid = p_sourceid
        AND d.dataflowflag = p_dataflowflag
        AND d.targetobject = p_targetobject;

    -- Get source count
    SELECT COUNT(1) INTO srccnt FROM {source_schema}.{source_table};

    -- Perform UPDATE based on dynamically matched columns
    UPDATE {target_schema}.{target_table} t
    SET
        {update_set_clause}
    FROM {source_schema}.{source_table} s
    WHERE {match_conditions};

    GET DIAGNOSTICS updcnt = ROW_COUNT;

    -- Perform INSERT using LEFT JOIN instead of NOT EXISTS
    INSERT INTO {target_schema}.{target_table} ({insert_columns})
    SELECT {insert_values}
    FROM {source_schema}.{source_table} s
    LEFT JOIN {target_schema}.{target_table} t ON {match_conditions}
    WHERE {insert_where_condition};

    GET DIAGNOSTICS inscnt = ROW_COUNT;

EXCEPTION WHEN others THEN
    GET STACKED DIAGNOSTICS
        p_errorid = RETURNED_SQLSTATE,
        p_errordesc = MESSAGE_TEXT;
    
    CALL ods.usp_etlerrorinsert(p_sourceid, p_targetobject, p_dataflowflag, p_batchid, p_taskname, 'sp_ExceptionHandling', p_errorid, p_errordesc, NULL);

    SELECT 0 INTO inscnt;
    SELECT 0 INTO updcnt;
END;
$procedure$;
"""
    return sp_template
