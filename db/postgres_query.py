# ******************************************************************
# To find sp in DB ⇩

find_sp_errorinsert = '''
                            SELECT SPECIFIC_NAME
                            FROM INFORMATION_SCHEMA.ROUTINES
                            WHERE ROUTINE_NAME = 'usp_etlerrorinsert'
                          '''

find_sp_postprocess = '''
                            SELECT SPECIFIC_NAME
                            FROM INFORMATION_SCHEMA.ROUTINES
                            WHERE ROUTINE_NAME = 'usp_etlpostprocess'
                          '''

find_sp_preprocess = '''
                            SELECT SPECIFIC_NAME
                            FROM INFORMATION_SCHEMA.ROUTINES
                            WHERE ROUTINE_NAME = 'usp_etlpreprocess'
                          '''

find_sp_usp_SrcToMain_lookup = '''
                            SELECT SPECIFIC_NAME
                            FROM INFORMATION_SCHEMA.ROUTINES
                            WHERE ROUTINE_NAME = 'usp_SrcToMain_lookup'
                          '''



# ******************************************************************
# To create sp (ETL Process) ⇩

postgres_sp_etlerrorinsert = """
CREATE OR REPLACE PROCEDURE ods.usp_etlerrorinsert(
	IN p_sourceid character varying,
	IN p_targetobject character varying,
	IN p_dataflowflag character varying,
	IN p_batchid integer,
	IN p_taskname character varying,
	IN p_packagename character varying,
	IN p_errorid integer,
	IN p_errordesc character varying,
	IN p_errorline integer)
LANGUAGE 'plpgsql'
AS $BODY$

DECLARE p_Flag1			INT ;
		p_Flag2			VARCHAR(100);
    
BEGIN
	
-- SQLINES DEMO ***  with Status, Source, Target, Inserted and Updated record counts

	IF ( SELECT 1 FROM  ODS.ControlDetail 
				WHERE TargetObject	= p_TargetObject   
				AND SourceId		= p_SourceId  
				AND DataflowFlag	= p_DataflowFlag  ) = 1
	THEN

	-- SQLINES LICENSE FOR EVALUATION USE ONLY
		INSERT INTO ODS.Error
				   (SourceName						,SourceType
				   ,SourceDescription				,SourceId
				   ,SourceObject					,DataflowFlag
				   ,TargetName						,TargetSchemaName
				   ,TargetObject					,TargetProcedureName
				   ,TaskName						,PackageName
				   ,JobName							,ErrorID
				   ,ErrorDesc						,ErrorLine
				   ,ErrorDate						,LatestBatchId)
		SELECT 
				    SourceName						,SourceType
				   ,SourceDescription				,SourceId
				   ,SourceObject					,DataflowFlag
				   ,TargetName						,TargetSchemaName
				   ,TargetObject					,TargetProcedureName
				   ,p_TaskName						,p_PackageName
				   ,JobName							,p_ErrorId
				   ,p_ErrorDesc						,p_ErrorLine
				   ,NOW()							,LatestBatchId
		FROM ODS.ControlDetail
		WHERE 	TargetObject 	= p_TargetObject   
		AND 	SourceId 		= p_SourceId  
		AND 	DataflowFlag 	= p_DataflowFlag;

	END IF;

	-- SQLINES DEMO *** control Table

	UPDATE ODS.ControlDetail
	SET		FlowStatus 	= 'Failed'    
	WHERE 	TargetObject 	= p_TargetObject   
	AND 	SourceId 		= p_SourceId  
	AND 	DataflowFlag 	= p_DataflowFlag 
    AND     latestbatchid   = p_batchid;
    
	UPDATE ODS.ControlHeader
	SET		Status 	= 'Failed'    
	WHERE 	SourceId 		= p_SourceId;    

	-- SQLINES DEMO ***  Audit Table
	UPDATE ODS.Audit
	SET		FlowStatus 	= 'Failed'     
	WHERE 	TargetObject 	= p_TargetObject   
	AND 	SourceId 		= p_SourceId  
	AND 	DataflowFlag 	= p_DataflowFlag
    AND     latestbatchid   = p_batchid;  
	
	IF p_dataflowflag = 'SLVtoGLD'
	THEN
		INSERT INTO ODS.Error
				   (SourceName						,SourceType
				   ,SourceDescription				,SourceId
				   ,SourceObject					,DataflowFlag
				   ,TargetName						,TargetSchemaName
				   ,TargetObject					,TargetProcedureName
				   ,TaskName						,PackageName
				   ,JobName							,ErrorID
				   ,ErrorDesc						,ErrorLine
				   ,ErrorDate						,LatestBatchId)
		SELECT 
				    'onesource'						,'PostgreSQL'
				   ,'PostgreSQL'					,p_sourceid
				   ,p_sourceid						,p_dataflowflag
				   ,'click'							,'click'
				   ,p_targetobject					,NULL
				   ,p_TaskName						,p_PackageName
				   ,NULL							,p_ErrorId
				   ,p_ErrorDesc						,p_ErrorLine
				   ,NOW()							,p_batchid;
				   
			END IF;

END;
$BODY$;
"""

postgres_sp_etlpostprocess = """
CREATE OR REPLACE PROCEDURE ods.usp_etlpostprocess(
	IN psourceid character varying,
	IN ptargetobject character varying,
	IN pdataflowflag character varying,
	IN sourcegroupflag integer,
	IN pbatchid integer,
	IN psourcecount integer,
	IN pinsertcount integer,
	IN pupdatecount integer)
LANGUAGE 'plpgsql'
AS $BODY$
  
--Declare variables  
  
DECLARE   
	vSrcDelCnt 				INT := 0;  
	IsFailed 				INT :=0;
	ErrorId					VARCHAR(50);
	ErrorDesc				VARCHAR(2000);
	ErrorLine				VARCHAR(2000);
	BRNtoSLVLastRunDate 		TIMESTAMP(3);
	DepSourceLastRunDate 	TIMESTAMP(3);
	DepSource 				VARCHAR(40); 
	DepSourceStatus 		VARCHAR(40);
	v_etllastrundate 		TIMESTAMP(3);
	deletecount 			INTEGER;
	flag1 					INTEGER;
	flag2 					CHARACTER;
BEGIN

-- SQLINES DEMO ***  Error Table
-- SQLINES LICENSE FOR EVALUATION USE ONLY
	SELECT 
		COALESCE(COUNT(*),0) INTO IsFailed 
	FROM ODS.Error
	WHERE LatestBatchId = pbatchid  
	AND TargetObject = ptargetobject  
	AND SourceId = psourceid  
	AND DataflowFlag = pdataflowflag;
	
	SELECT NOW()::TIMESTAMP INTO v_etllastrundate;

IF IsFailed = 0 
THEN 	
	IF (pdataflowflag = 'SRCtoBRN')  
		THEN  
			-- SQLINES DEMO *** ADER FOR STATUS
		UPDATE ODS.ControlDetail
		SET     IsReadyForExecution		= 0  
		WHERE   SourceId				= psourceid  
		AND     DataflowFlag			= 'SRCtoBRN'  
		AND     Isapplicable			= 1;
        
		UPDATE ODS.ControlHeader
		SET		Status				=	'Started',
				LastUpdatedDate		=	NOW()
		WHERE	SourceId			=	psourceid 
		AND		Isapplicable		=	1;
		
		UPDATE ODS.ControlDetail
		SET     IsReadyForExecution		= 1  
		WHERE   SourceId				= psourceid  
		AND     DataflowFlag			= 'BRNtoSLV'  
		AND     Isapplicable			= 1;
	END IF;		
  
	IF (pdataflowflag = 'BRNtoSLV')  
	THEN  

		-- SQLINES DEMO *** ADER FOR STATUS

		UPDATE ODS.ControlHeader
		SET		Status				=	'Completed',
				LastUpdatedDate		=	NOW()
		WHERE	SourceId			=	psourceid 
		AND		Isapplicable		=	1;

		  
		UPDATE ODS.ControlDetail
		SET		IsReadyForExecution 	= 0
		WHERE 	TargetObject 			= ptargetobject
		AND 	SourceId 				= psourceid  
		AND 	DataflowFlag			= 'BRNtoSLV'  
		AND 	Isapplicable			= 1;

		UPDATE ODS.ControlDetail
		SET     IsReadyForExecution		= 1
		WHERE   SourceId				= psourceid  
		AND     DataflowFlag			= 'SRCtoBRN'  
		AND     Isapplicable			= 1;
		
		IF sourcegroupflag =  1 AND pdataflowflag = 'BRNtoSLV'
		THEN
		
		update ods.sourcegroupingdtl
		set status 				= 'Completed',
			lastrundatetime		=	NOW()::timestamp,
			loadenddatetime = NOW()::timestamp
			where sourceid = psourceid
			and schemaname = 'dwh'
			and isapplicable = 1;
			
		END IF;		   

	END IF; 

	-- SQLINES DEMO *** le with Status and Updated date  
  
	UPDATE ODS.ControlDetail
	SET		FlowStatus 		= 'Completed',
			EtlLastRunDate 	= v_etllastrundate
	WHERE 	LatestBatchId 	= pbatchid  
	AND 	TargetObject 	= ptargetobject   
	AND 	SourceId 		= psourceid  
	AND 	DataflowFlag 	= pdataflowflag  
	AND 	Isapplicable 	= 1;

	UPDATE ODS.Audit 
	SET		SrcRwCnt 		= psourcecount,
			TgtInscnt 		= pinsertcount,
			TgtUpdcnt 		= pupdatecount,
			FlowStatus 		= 'Completed',
			UpdatedDate 	= Now(),
			LoadEndTime 	= Now()  
	WHERE 	LatestBatchId 	= pbatchid  
	AND 	TargetObject 	= ptargetobject  
	AND 	SourceId 		= psourceid  
	AND 	DataflowFlag 	= pdataflowflag; 

	ELSE 

		-- SQLINES DEMO *** le with Status and Updated date  

		UPDATE ODS.ControlHeader
		SET		Status				=	'Failed',
				LastUpdatedDate		=	NOW()
		WHERE	SourceId			=	psourceid 
		AND		Isapplicable		=	1;
  
		UPDATE ODS.ControlDetail 
		SET		FlowStatus 			= 'Failed'     
		WHERE 	LatestBatchId 		= pbatchid  
		AND 	TargetObject 		= ptargetobject   
		AND 	SourceId 			= psourceid  
		AND 	DataflowFlag 		= pdataflowflag  
		AND 	Isapplicable 		= 1;

		
		UPDATE ODS.Audit
	    SET		SrcRwCnt 		= psourcecount,
			    TgtInscnt 		= pinsertcount,		    
			    TgtUpdcnt 		= pupdatecount,
				FlowStatus 		= 'Failed',
				UpdatedDate 	= Now(),
				LoadEndTime 	= Now()  
		WHERE 	LatestBatchId 	= pbatchid  
		AND 	TargetObject 	= ptargetobject  
		AND 	SourceId 		= psourceid  
		AND 	DataflowFlag 	= pdataflowflag;
		
		IF sourcegroupflag =  1
	THEN		
		update ods.sourcegroupingdtl
		set status = 'Failed',
		loadenddatetime = NOW()::timestamp
		where sourceid = psourceid
		and schemaname = 'dwh'
		and isapplicable = 1;
		
	END IF;
  

	END IF;

END;
$BODY$;
"""

postgres_sp_etlpreprocess = """


        CREATE OR REPLACE PROCEDURE ods.usp_etlpreprocess(
	IN psourceid character varying,
	IN ptargetobject character varying,
	IN pdataflowflag character varying,
	IN sourcegroupflag integer,
	IN sourcecount integer,
	IN useragent character varying,
	IN etlbatchid character varying,
	INOUT batchid integer)
LANGUAGE 'plpgsql'
AS $BODY$

	DECLARE Sql1 			TEXT;
			Stagetable 		VARCHAR(100);
			ErrorId			VARCHAR(50);
			ErrorDesc		VARCHAR(2000);
			ErrorLine		VARCHAR(2000);
			TargetSchema  	VARCHAR(400);	
			Targettable		VARCHAR(400);	
			vSourceName		VARCHAR(100);
	        loadstarttime   timestamp without time zone;
	        executionflag   integer;
	        flag1           integer;
			_row_found 		bool;
	        flag2           character varying;            

BEGIN

	ExecutionFlag := 1;

	-- SQLINES LICENSE FOR EVALUATION USE ONLY
	SELECT IsReadyForExecution,
		  TargetSchemaName || '.' || TargetObject,TargetObject,
		  SourceName INTO ExecutionFlag, TargetSchema,Targettable, vSourceName
	FROM ods.ControlDetail
	WHERE SourceId		= psourceid
	AND   TargetObject	= ptargetobject
	AND   DataflowFlag	= pdataflowflag
	AND   Isapplicable 	= 1; 	
	
	
	--TRUNCATE TABLE targetobject;
    IF (pdataflowflag = 'SRCtoBRN')  
	THEN     
		EXECUTE 'SELECT EXISTS (SELECT FROM pg_tables ' ||
				'where tablename = ''' || Targettable || ''')'
      INTO _row_found;

      	IF _row_found THEN   
        	EXECUTE 'TRUNCATE ' || TargetSchema;
		END IF;	
	END IF;

    IF (pdataflowflag = 'BRNtoSLV')  
	THEN  
		SELECT  
            SourceObject INTO Stagetable
        FROM ODS.ControlDetail
        WHERE SourceId = psourceid 
		AND DataflowFlag = pdataflowflag 
        AND TargetObject = ptargetobject
		AND Isapplicable = 1;
		
	END IF;
-- SQLINES DEMO *** om Control table

	SELECT 
		COALESCE(LatestBatchId,0) + 1 AS Batch INTO BatchId 
	FROM ODS.ControlDetail
	WHERE SourceId = psourceid 
	AND TargetObject = ptargetobject
	AND DataflowFlag = pdataflowflag
	AND Isapplicable = 1; 
	
	LoadStartTime := now();

-- SQLINES DEMO *** Control Header

	UPDATE ODS.ControlHeader
	SET		Status			=	'Started',
			LastUpdatedDate	=	NOW()
	WHERE	SourceId		=	psourceid 
	AND		Isapplicable	=	1;

-- SQLINES DEMO *** Control Table

	UPDATE ODS.ControlDetail
	SET LatestBatchId 	= BatchId,
		FlowStatus 		= 'Started'
	WHERE SourceId 		= psourceid 
	AND TargetObject 	= ptargetobject
	AND DataflowFlag 	= pdataflowflag
	and Isapplicable 	= 1;

-- SQLINES DEMO ***  with Status, Source, Target, Inserted and Updated record counts

-- SQLINES LICENSE FOR EVALUATION USE ONLY
	INSERT INTO ODS.Audit
			   (SourceName          ,Sourcetype			,SourceDescription         	,SourceObject
			   ,SourceId			,DataflowFlag       ,SrcRwCnt					,SrcDelCnt           
			   ,TgtInscnt           ,TgtDelcnt			,TgtUpdcnt				   	,FlowStatus
			   ,TargetName          ,TargetSchemaName   ,TargetObject				,TargetProcedureName
			   ,CreatedDate         ,UpdatedDate        ,LoadStartTime			   	,LoadEndTime
			   ,LatestBatchId		,LoadType			,etlbatchid					,useragent		,sourcegroupflag)
	SELECT		SourceName	        ,Sourcetype	        ,SourceDescription	        ,SourceObject
			   ,SourceId	        ,DataflowFlag	    ,SourceCount				,0
			   ,NULL				,NULL				,NULL						,'Started'
			   ,TargetName          ,TargetSchemaName   ,TargetObject				,TargetProcedureName
			   ,Now()				,NULL				,LoadStartTime				,NULL
			   ,BatchId				,LoadType			,etlbatchid					,useragent		,sourcegroupflag
	FROM ODS.ControlDetail
	WHERE SourceId = psourceid 
	AND TargetObject = ptargetobject
	AND DataflowFlag = pdataflowflag
	and Isapplicable = 1;
	
	IF (sourcegroupflag =  1 AND pdataflowflag = 'SRCtoBRN')
	THEN
	
	update ods.sourcegroupingdtl
	set status = 'Started',
	loadstartdatetime = NOW()::timestamp
	where sourceid = psourceid
	and schemaname = 'dwh'
	and isapplicable = 1;
	
	END IF;
	
	select BatchId INTO batchid;

END;
$BODY$;
"""

postgres_sp_SRCtoMAIN_lookup = """
CREATE OR REPLACE PROCEDURE usp_SrcToMain_lookup()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert transformed schema into main_lookup
    INSERT INTO ods.main_lookup (
        column_id, column_name,  target_table,
        target_data_type, length, precisions, scale, nullable,
        key_constraint, processing_layer, default_value
    )
    SELECT
	    s.column_id,
	    s.column_name,
	    s.target_table,
	    COALESCE(m.targetdatatypes, 'text') AS target_data_type, -- Use mapping, else text
	    CASE
            WHEN s.length = -1 THEN NULL -- Replace -1 with NULL
            ELSE s.length
        END AS length,
	    s.precisions,
	    s.scale,
	    s.nullable,
	    s.key_constraint,
	    'BRN' AS processing_layer, -- Modify based on ETL layer (e.g., 'raw', 'staging', 'gold')
	    NULL AS default_value -- Modify if you need to derive default values
	FROM
	    ods.source_lookup s
	LEFT JOIN
	    (SELECT DISTINCT sourcedatatypes, targetdatatypes, sourcedatabasetype
	     FROM ods.datatype_mapping) m
	ON
	    s.source_data_type = LOWER(m.sourcedatatypes)
	    AND s.source_type = m.sourcedatabasetype
	WHERE
	    s.column_id IS NOT NULL -- Ensure column_id is not null to prevent duplicates
	ORDER BY
	    s.column_id, s.column_name, s.target_table;
 
 
END;
$$;
"""


data = [
    # SQL Server to PostgreSQL
    ("SQL Server", "PostgreSQL", "INT", "INTEGER"),
    ("SQL Server", "PostgreSQL", "BIGINT", "BIGINT"),
    ("SQL Server", "PostgreSQL", "SMALLINT", "SMALLINT"),
    ("SQL Server", "PostgreSQL", "TINYINT", "SMALLINT"),
    ("SQL Server", "PostgreSQL", "BIT", "TEXT"),
    ("SQL Server", "PostgreSQL", "DECIMAL", "NUMERIC"),
    ("SQL Server", "PostgreSQL", "NUMERIC", "NUMERIC"),
    ("SQL Server", "PostgreSQL", "FLOAT", "DOUBLE PRECISION"),
    ("SQL Server", "PostgreSQL", "REAL", "REAL"),
    ("SQL Server", "PostgreSQL", "MONEY", "NUMERIC(19,4)"),
    ("SQL Server", "PostgreSQL", "SMALLMONEY", "NUMERIC(10,4)"),
    ("SQL Server", "PostgreSQL", "VARCHAR", "VARCHAR"),
    ("SQL Server", "PostgreSQL", "NVARCHAR", "VARCHAR"),
    ("SQL Server", "PostgreSQL", "TEXT", "TEXT"),
    ("SQL Server", "PostgreSQL", "NTEXT", "TEXT"),
    ("SQL Server", "PostgreSQL", "CHAR", "CHAR"),
    ("SQL Server", "PostgreSQL", "NCHAR", "CHAR"),
    ("SQL Server", "PostgreSQL", "DATETIME", "TIMESTAMP"),
    ("SQL Server", "PostgreSQL", "DATETIME2", "TIMESTAMP"),
    ("SQL Server", "PostgreSQL", "SMALLDATETIME", "TIMESTAMP"),
    ("SQL Server", "PostgreSQL", "DATE", "DATE"),
    ("SQL Server", "PostgreSQL", "TIME", "TIME"),
    ("SQL Server", "PostgreSQL", "BINARY", "BYTEA"),
    ("SQL Server", "PostgreSQL", "VARBINARY", "BYTEA"),
    ("SQL Server", "PostgreSQL", "IMAGE", "BYTEA"),
    ("SQL Server", "PostgreSQL", "UNIQUEIDENTIFIER", "UUID"),
    ("SQL Server", "PostgreSQL", "XML", "TEXT"),
    ("SQL Server", "PostgreSQL", "SQL_VARIANT", "TEXT"),
    ("SQL Server", "PostgreSQL", "HIERARCHYID", "TEXT"),
    ("SQL Server", "PostgreSQL", "GEOGRAPHY", "TEXT"),
    ("SQL Server", "PostgreSQL", "GEOMETRY", "TEXT"),

    # PostgreSQL to PostgreSQL
    ("PostgreSQL", "PostgreSQL", "SMALLINT", "SMALLINT"),
    ("PostgreSQL", "PostgreSQL", "INTEGER", "INTEGER"),
    ("PostgreSQL", "PostgreSQL", "BIGINT", "BIGINT"),
    ("PostgreSQL", "PostgreSQL", "DECIMAL", "DECIMAL"),
    ("PostgreSQL", "PostgreSQL", "NUMERIC", "NUMERIC"),
    ("PostgreSQL", "PostgreSQL", "REAL", "REAL"),
    ("PostgreSQL", "PostgreSQL", "DOUBLE PRECISION", "DOUBLE PRECISION"),
    ("PostgreSQL", "PostgreSQL", "SERIAL", "SERIAL"),
    ("PostgreSQL", "PostgreSQL", "BIGSERIAL", "BIGSERIAL"),
    ("PostgreSQL", "PostgreSQL", "BOOLEAN", "BOOLEAN"),
    ("PostgreSQL", "PostgreSQL", "CHAR", "CHAR"),
    ("PostgreSQL", "PostgreSQL", "VARCHAR", "VARCHAR"),
    ("PostgreSQL", "PostgreSQL", "TEXT", "TEXT"),
    ("PostgreSQL", "PostgreSQL", "BYTEA", "BYTEA"),
    ("PostgreSQL", "PostgreSQL", "TIMESTAMP", "TIMESTAMP"),
    ("PostgreSQL", "PostgreSQL", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"),
    ("PostgreSQL", "PostgreSQL", "DATE", "DATE"),
    ("PostgreSQL", "PostgreSQL", "TIME", "TIME"),
    ("PostgreSQL", "PostgreSQL", "TIME WITH TIME ZONE", "TIME WITH TIME ZONE"),
    ("PostgreSQL", "PostgreSQL", "INTERVAL", "INTERVAL"),
    ("PostgreSQL", "PostgreSQL", "UUID", "UUID"),
    ("PostgreSQL", "PostgreSQL", "XML", "XML"),
    ("PostgreSQL", "PostgreSQL", "JSON", "JSON"),
    ("PostgreSQL", "PostgreSQL", "JSONB", "JSONB"),
    ("PostgreSQL", "PostgreSQL", "ARRAY", "ARRAY"),
    ("PostgreSQL", "PostgreSQL", "CITEXT", "CITEXT"),

    # Flatfile to PostgreSQL
    ("Flatfile", "PostgreSQL", "int64", "BIGINT"),
    ("Flatfile", "PostgreSQL", "int32", "INTEGER"),
    ("Flatfile", "PostgreSQL", "float64", "DOUBLE PRECISION"),
    ("Flatfile", "PostgreSQL", "float32", "REAL"),
    ("Flatfile", "PostgreSQL", "bool", "BOOLEAN"),
    ("Flatfile", "PostgreSQL", "object", "TEXT"),
    ("Flatfile", "PostgreSQL", "string", "TEXT"),
    ("Flatfile", "PostgreSQL", "category", "TEXT"),
    ("Flatfile", "PostgreSQL", "datetime64[ns]", "TIMESTAMP WITHOUT TIME ZONE"),
    ("Flatfile", "PostgreSQL", "datetime64[ns, tz]", "TIMESTAMP WITH TIME ZONE"),
    ("Flatfile", "PostgreSQL", "timedelta[ns]", "INTERVAL"),
]

golden_layer_sp = """
CREATE OR REPLACE FUNCTION ods.clickhouse_table_script_generator(
	p_target_database character varying,
	p_source_schema_name character varying,
	p_source_table_name character varying)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_table_ddl   text;
    column_record record;
	v_primary_key text;
BEGIN
	select column_name into v_primary_key  from information_schema.columns
	where table_name ~ ('^('||p_source_table_name||')$')
	and table_schema::name  ~ ('^('||p_source_schema_name||')$')
	and is_identity = 'YES';
 
    FOR column_record IN
 
SELECT t.table_name as table_name,
    c.table_schema as  schema_name,
    c.column_name as column_name,
    d.targetdatatypes as column_type ,
	min(ordinal_position) as min_attnum,
	max(ordinal_position) as max_attnum,
	ordinal_position
   FROM information_schema.tables t
     JOIN information_schema.columns c
	 ON t.table_name::name = c.table_name::name
	 AND t.table_schema::name = c.table_schema::name
	 JOIN ods.datatype_mapping d
	 on c.data_type = d.sourcedatatypes
	where t.table_name ~ ('^('||p_source_table_name||')$')
	and t.table_schema::name  ~ ('^('||p_source_schema_name||')$')
	group by t.table_name,c.table_schema,c.column_name,d.targetdatatypes,ordinal_position
	ORDER BY ordinal_position
    LOOP
        IF column_record.min_attnum = 1 THEN
            v_table_ddl:='CREATE TABLE '||p_target_database||'.'||column_record.table_name||' (';
        ELSE
            v_table_ddl:=v_table_ddl||',';
        END IF;
 
        IF column_record.min_attnum <= column_record.max_attnum THEN
            v_table_ddl:=v_table_ddl||chr(10)||
                     '    '||column_record.column_name||' '||CASE WHEN column_record.column_name = v_primary_key THEN 'Int64' ELSE column_record.column_type END;
        END IF;
    END LOOP;
 
    v_table_ddl:=v_table_ddl||') ENGINE = MergeTree ORDER BY tuple('||v_primary_key||');';
    RETURN v_table_ddl;
END;
$BODY$;
 
ALTER FUNCTION ods.clickhouse_table_script_generator(character varying, character varying, character varying)
    OWNER TO postgres;

"""