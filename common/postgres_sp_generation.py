from sqlalchemy import create_engine, text

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

def find_matching_columns(source_columns, target_columns, keys):
    """
    Find the best match for 'id', 'name', 'email' even if prefixed/suffixed in source and target.
    """
    matched = {}
    for key in keys:
        for col in source_columns:
            if key in col.lower():  # Check if key exists as substring in column name
                for tcol in target_columns:
                    if key in tcol.lower():  # Match only if target has similar column
                        matched[key] = (col, tcol)
                        break
    return matched

def generate_stored_procedure(engine, target_schema='dwh', target_table=None, source_schema='stg', source_table=None):
    """Generate a PostgreSQL Stored Procedure dynamically based on table structure."""
    db_type = detect_database_type(engine)
    if db_type != "postgresql":
        raise ValueError("Only PostgreSQL is supported for SP generation.")

    # Fetch columns for INSERT and UPDATE
    target_columns = fetch_table_columns(engine, target_schema, target_table)
    source_columns = fetch_table_columns(engine, source_schema, source_table)

    # Match 'id', 'name', 'email' even if prefixed/suffixed
    match_keys = ["id", "name", "email"]
    matched_columns = find_matching_columns(source_columns, target_columns, match_keys)

    if not matched_columns:
        raise ValueError(f"No suitable match columns found for {source_table} and {target_table}.")

    match_conditions = " AND ".join([f"t.{tcol} = s.{scol}" for _, (scol, tcol) in matched_columns.items()])

    # Generate UPDATE SET clause from source columns
    update_columns = [col for col in target_columns if col in source_columns]
    update_set_clause = ",\n        ".join([f"{col} = s.{col}" for col in update_columns] + [
        "etlactiveind = 1",
        "etljobname = p_etljobname",
        "envsourcecd = p_envsourcecd",
        "datasourcecd = p_datasourcecd",
        "etlupdateddatetime = NOW()"
    ])

    # Generate INSERT column names and values (excluding computed ETL columns)
    target_columns = [col for col in target_columns if not col.endswith("key")]
    insert_columns = ", ".join(target_columns)
    insert_values = ", ".join([f"s.{col}" if col in source_columns else f"p_{col}" for col in target_columns])

    # Generate the stored procedure
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

    -- Perform UPDATE based on selected match keys
    UPDATE {target_schema}.{target_table} t
    SET
        {update_set_clause}
    FROM {source_schema}.{source_table} s
    WHERE {match_conditions};

    GET DIAGNOSTICS updcnt = ROW_COUNT;

    -- Perform INSERT for records not fully matching
    INSERT INTO {target_schema}.{target_table} ({insert_columns})
    SELECT {insert_values}
    FROM {source_schema}.{source_table} s
    WHERE NOT EXISTS (
        SELECT 1 FROM {target_schema}.{target_table} t WHERE {match_conditions}
    );

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


# Example Usage
# engine = create_engine("postgresql://user:password@localhost:5432/mydatabase")  # Replace with actual DB engine
# sp_sql = generate_stored_procedure(engine, "dwh", "d_customer", "stg", "stg_customer")

# print(sp_sql)
