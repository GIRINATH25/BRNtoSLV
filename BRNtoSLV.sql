-- DROP PROCEDURE ods.upsert_main_lookup(varchar, varchar, varchar, varchar);

CREATE OR REPLACE PROCEDURE ods.upsert_main_lookup(IN p_sourceid character varying, IN p_tablename character varying, IN p_schemaname character varying, IN p_targettablename character varying)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    rec RECORD;
    v_default_value VARCHAR(255);
BEGIN
    FOR rec IN 
        SELECT 
            col.column_name,
            col.data_type,
            col.character_maximum_length AS length,
            col.numeric_precision AS precisions,
            col.numeric_scale AS scale,
            col.is_nullable,
            COALESCE((
                SELECT STRING_AGG(tc.constraint_type, ', ') 
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = p_schemaname 
                AND tc.table_name = p_tablename
                AND kcu.column_name = col.column_name
            ), NULL) AS key_constraint
        FROM information_schema.columns col
        WHERE col.table_schema = p_schemaname AND col.table_name = p_tablename
    LOOP
        -- Determine default_value based on data_type
        CASE 
            WHEN rec.data_type IN ('integer', 'bigint', 'smallint') THEN 
                v_default_value := '0';
            WHEN rec.data_type IN ('decimal', 'numeric', 'float', 'real', 'double precision') THEN 
                v_default_value := '0.00';
            WHEN rec.data_type IN ('character varying', 'varchar', 'text', 'character', 'char') THEN 
                v_default_value := '''''';  -- Empty string ''
            WHEN rec.data_type IN ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone', 'time') THEN 
                v_default_value := '1970-01-01';
            ELSE 
                v_default_value := NULL;
        END CASE;

        -- Check if the column already exists in ods.main_lookup
        IF EXISTS (
            SELECT 1 FROM ods.main_lookup 
            WHERE sourceid = p_sourceid 
            AND source_tablename = p_tablename
            AND source_columnname = rec.column_name
        ) THEN
            -- Update existing record
            UPDATE ods.main_lookup
            SET 
                silverdwh_columnname = rec.column_name,
                datatype = rec.data_type,
                length = rec.length,
                precisions = rec.precisions,
                scale = rec.scale,
                "nullable" = rec.is_nullable,
                keyconstraint = rec.key_constraint,
                silver_default_value = v_default_value,
                createdatetime = CURRENT_TIMESTAMP
            WHERE sourceid = p_sourceid 
            AND source_tablename = p_tablename
            AND source_columnname = rec.column_name;
        ELSE
            -- Insert new record
            INSERT INTO ods.main_lookup (
                sourceid, 
                silverdwh_tablename, 
                silverdwh_columnname, 
                datatype, 
                length, 
                precisions, 
                scale, 
                "nullable", 
                keyconstraint, 
                silver_default_value, 
                createdatetime
            ) 
            VALUES (
                p_sourceid, 
                p_targettablename, 
                rec.column_name, 
                rec.data_type, 
                rec.length, 
                rec.precisions, 
                rec.scale, 
                rec.is_nullable, 
                rec.key_constraint, 
                v_default_value, 
                CURRENT_TIMESTAMP
            );
        END IF;
    END LOOP;
END;
$procedure$
;












-- DROP PROCEDURE ods.create_slv_table_from_lookup(text, text, text);

CREATE OR REPLACE PROCEDURE ods.create_slv_table_from_lookup(IN p_schemaname text, IN p_srctablename text, IN p_targettablename text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_sql TEXT;
    rec RECORD;
    v_primary_key_columns TEXT := '';
    v_primary_key_column_name TEXT;
BEGIN
    -- Construct the CREATE TABLE statement dynamically
    v_sql := 'CREATE TABLE '|| p_schemaname || '.' || p_targettablename || ' (';
    
    -- Generate unique identity column
    v_primary_key_column_name := split_part(p_targettablename, '_', 2) || 'key';
    v_sql := v_sql || v_primary_key_column_name || ' BIGINT GENERATED ALWAYS AS IDENTITY '
            || '(INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL, ';
    
    FOR rec IN (
        SELECT silverdwh_columnname, datatype, length, precisions, scale, nullable, keyconstraint, silver_default_value
        FROM ods.main_lookup
        WHERE silverdwh_tablename = p_targettablename
    ) LOOP
        v_sql := v_sql || rec.silverdwh_columnname || ' ' || rec.datatype;
        
        -- Add length/precision/scale if applicable
        IF rec.datatype ILIKE 'char%' OR rec.datatype ILIKE 'varchar' THEN
            v_sql := v_sql || '(' || COALESCE(rec.length::TEXT, '255') || ')';
        ELSIF rec.datatype ILIKE 'decimal' OR rec.datatype ILIKE 'numeric' THEN
            v_sql := v_sql || '(' || COALESCE(rec.precisions::TEXT, '18') || ', ' || COALESCE(rec.scale::TEXT, '2') || ')';
        END IF;
        
        -- Add NULL/NOT NULL constraint
        IF rec.nullable::BOOLEAN = FALSE THEN
            v_sql := v_sql || ' NOT NULL';
        END IF;
        
        -- Add DEFAULT value if present
--        IF rec.silver_default_value IS NOT NULL THEN
--            v_sql := v_sql || ' DEFAULT ' || quote_literal(rec.silver_default_value);
--        END IF;
--        
        -- Capture primary key columns from lookup table for UNIQUE constraint
        IF UPPER(rec.keyconstraint) ILIKE '%PRIMARY%' OR UPPER(rec.keyconstraint) ILIKE '%UNIQUE%' THEN  -- FIXED CHECK FOR PRIMARY KEY
            v_primary_key_columns := v_primary_key_columns || rec.silverdwh_columnname || ', ';
        END IF;
        
        v_sql := v_sql || ', ';
    END LOOP;
    
    -- Add additional columns
    v_sql := v_sql || 'etlactiveind INT, ';
    v_sql := v_sql || 'etljobname VARCHAR(200), ';
    v_sql := v_sql || 'envsourcecd VARCHAR(50), ';
    v_sql := v_sql || 'datasourcecd VARCHAR(50), ';
    v_sql := v_sql || 'etlcreateddatetime TIMESTAMP DEFAULT now(), ';
    v_sql := v_sql || 'etlupdateddatetime TIMESTAMP, ';
    
    -- Remove the last comma and space
    v_sql := rtrim(v_sql, ', ');

    -- Add PRIMARY KEY constraint only on the generated key column
    v_sql := v_sql || ', PRIMARY KEY (' || v_primary_key_column_name || ')';
    
    -- Add UNIQUE constraint on lookup table primary key columns
    IF v_primary_key_columns IS NOT NULL AND v_primary_key_columns <> '' THEN
        v_primary_key_columns := rtrim(v_primary_key_columns, ', ');  -- FIXED TRAILING COMMA
        v_sql := v_sql || ', UNIQUE (' || v_primary_key_columns || ')';
    END IF;
    
    v_sql := v_sql || ');';
    
    -- Execute the generated SQL statement
    EXECUTE v_sql;
END;
$procedure$
;











-- DROP PROCEDURE ods.usp_sp_generate_slv(varchar, varchar, varchar, varchar);

CREATE OR REPLACE PROCEDURE ods.usp_sp_generate_slv(IN p_target_schema character varying, IN p_target_table character varying, IN p_source_schema character varying, IN p_source_table character varying)
 LANGUAGE plpgsql
AS $procedurex$
DECLARE
    p_procname TEXT;
    sql_script TEXT;
    match_conditions TEXT;
    insert_where_condition TEXT;
    update_set_clause TEXT;
    insert_columns TEXT;
    insert_values TEXT;
    key_columns TEXT;
BEGIN
    p_procname := 'usp_' || p_target_table;

    -- Fetch primary or unique key columns from source table
    SELECT COALESCE(string_agg(column_name, ', '), '1') -- Fallback to '1' if no key exists
    INTO key_columns
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE table_schema = p_source_schema
    AND table_name = p_source_table;

    -- Build match condition using key columns (for UPDATE, ON, and INSERT WHERE)
    IF key_columns = '1' THEN
        match_conditions := '1=1'; -- No keys found, fallback to 1=1
        insert_where_condition := '1=1';
    ELSE
        SELECT string_agg(format('t."%I" = s."%I"', column_name, column_name), ' AND '),string_agg(format('t."%I" IS NULL', column_name), ' AND ')
        INTO match_conditions,insert_where_condition
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = p_source_schema
        AND table_name = p_source_table
        AND column_name IN (SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                            WHERE table_schema = p_source_schema
                            AND table_name = p_source_table);

        -- INSERT WHERE condition (uses same keys but checks NULL on target)
--        SELECT string_agg(format('t."%I" IS NULL', column_name), ' AND ')
--        INTO insert_where_condition
--        FROM INFORMATION_SCHEMA.COLUMNS
--        WHERE table_schema = p_source_schema
--        AND table_name = p_source_table
--        AND column_name IN (SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
--                            WHERE table_schema = p_source_schema
--                            AND table_name = p_source_table);
    END IF;

    -- Determine insert columns and values
    SELECT string_agg(format('"%I"', column_name), ', '),
           string_agg(format('s."%I"', column_name), ', ')
    INTO insert_columns, insert_values
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = p_source_schema
    AND table_name = p_source_table;

    -- Define update set clause with additional fields
    SELECT string_agg(format('"%I" = s."%I"', column_name, column_name), ', ') ||
           ', etlactiveind = 1, etljobname = p_etljobname, envsourcecd = p_envsourcecd, datasourcecd = p_datasourcecd, etlupdateddatetime = NOW()'
    INTO update_set_clause
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = p_source_schema
    AND table_name = p_source_table;

    -- Generate the stored procedure dynamically
    sql_script := format(
        $SQL$
        CREATE OR REPLACE PROCEDURE %I.%I(
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
            p_errorid integer;
            p_errordesc character varying;
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
            SELECT COUNT(1) INTO srccnt FROM %I.%I;

            -- Perform UPDATE
            UPDATE %I.%I t
            SET
                %s
            FROM %I.%I s
            WHERE %s;

            GET DIAGNOSTICS updcnt = ROW_COUNT;

            -- Perform INSERT with additional fields
            INSERT INTO %I.%I (%s, etlactiveind, etljobname, envsourcecd, datasourcecd, etlupdateddatetime)
            SELECT %s, 1, p_etljobname, p_envsourcecd, p_datasourcecd, NOW()
            FROM %I.%I s
            LEFT JOIN %I.%I t ON %s
            WHERE %s;

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
        $SQL$,
        p_target_schema, p_procname,
        p_source_schema, p_source_table,
        p_target_schema, p_target_table, update_set_clause, p_source_schema, p_source_table, match_conditions,
        p_target_schema, p_target_table, insert_columns, insert_values, p_source_schema, p_source_table, p_target_schema, p_target_table, match_conditions, insert_where_condition
    );

    -- Execute the SQL to create the procedure
    EXECUTE sql_script;
END;
$procedurex$
;