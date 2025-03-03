# ******************************************************************
# To execute sp call ⇩
exec_sp_errorinsert = "CALL ods.usp_etlerrorinsert( :sourceid, :targetobject, :dataflowflag, :latestbatchid, :task, :package, :error_id, :error_desc, :error_line )"

exec_sp_etlpostprocess = "CALL ods.usp_etlpostprocess( :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :latestbatchid, :source_count, :insert_count, :update_count)"

exec_sp_etlpreprocess = "CALL ods.usp_etlpreprocess( :sourceid, :targetobject, :dataflowflag, :sourcegroupflag, :source_count, :user_agent, :etl_batch_id, NULL)"


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
	
	IF p_dataflowflag = 'DWtoClick'
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
	STGtoDWLastRunDate 		TIMESTAMP(3);
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
	IF (pdataflowflag = 'SRCtoStg')  
		THEN  
			-- SQLINES DEMO *** ADER FOR STATUS
		UPDATE ODS.ControlDetail
		SET     IsReadyForExecution		= 0  
		WHERE   SourceId				= psourceid  
		AND     DataflowFlag			= 'SRCtoStg'  
		AND     Isapplicable			= 1;
        
		UPDATE ODS.ControlHeader
		SET		Status				=	'Started',
				LastUpdatedDate		=	NOW()
		WHERE	SourceId			=	psourceid 
		AND		Isapplicable		=	1;
		
		UPDATE ODS.ControlDetail
		SET     IsReadyForExecution		= 1  
		WHERE   SourceId				= psourceid  
		AND     DataflowFlag			= 'StgtoDW'  
		AND     Isapplicable			= 1;
	END IF;		
  
	IF (pdataflowflag = 'StgtoDW')  
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
		AND 	DataflowFlag			= 'StgtoDW'  
		AND 	Isapplicable			= 1;

		UPDATE ODS.ControlDetail
		SET     IsReadyForExecution		= 1
		WHERE   SourceId				= psourceid  
		AND     DataflowFlag			= 'SRCtoStg'  
		AND     Isapplicable			= 1;
		
		IF sourcegroupflag =  1 AND pdataflowflag = 'StgtoDW'
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
    IF (pdataflowflag = 'SRCtoStg')  
	THEN     
		EXECUTE 'SELECT EXISTS (SELECT FROM pg_tables ' ||
				'where tablename = ''' || Targettable || ''')'
      INTO _row_found;

      	IF _row_found THEN   
        	EXECUTE 'TRUNCATE ' || TargetSchema;
		END IF;	
	END IF;

    IF (pdataflowflag = 'STGtoDW')  
	THEN  
		SELECT  
            SourceObject INTO Stagetable
        FROM ODS.ControlDetail
        WHERE SourceId = psourceid 
		AND DataflowFlag = pdataflowflag 
        AND TargetObject = ptargetobject
		AND Isapplicable = 1;
		
		Sql1 := N'SELECT @SourceCount  = COUNT(1) FROM OneSource_Stage.dbo.'||Stagetable;
		EXECUTE sp_executesql Sql1, N'@SourceCount NVARCHAR(100) OUTPUT', SourceCount = SourceCount OUTPUT;
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
	
	IF (sourcegroupflag =  1 AND pdataflowflag = 'SRCtoStg')
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


# ******************************************************************
# To create sp for data movement (basically script generation) ⇩

postgres_sp_generation_slv = """

CREATE OR REPLACE PROCEDURE :targetschemaname.usp_:targetobject(IN p_sourceid character varying, IN p_dataflowflag character varying, IN p_targetobject character varying, OUT srccnt integer, OUT inscnt integer, OUT updcnt integer, OUT dltcount integer, INOUT flag1 character varying, OUT flag2 character varying)
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    p_etljobname VARCHAR(100);
    p_envsourcecd VARCHAR(50);
    p_datasourcecd VARCHAR(50);
    p_batchid integer;
    p_taskname VARCHAR(100);
    p_packagename  VARCHAR(100);
    p_errorid integer;
    p_errordesc character varying;
    p_errorline integer;
    p_rawstorageflag integer;

BEGIN
    SELECT d.jobname, h.envsourcecode, h.datasourcecode, d.latestbatchid, d.targetprocedurename, h.rawstorageflag
    INTO p_etljobname, p_envsourcecd, p_datasourcecd, p_batchid, p_taskname, p_rawstorageflag
    FROM ods.controldetail d
    INNER JOIN ods.controlheader h
        ON d.sourceid = h.sourceid
    WHERE d.sourceid = p_sourceid
        AND d.dataflowflag = p_dataflowflag
        AND d.targetobject = p_targetobject;

    SELECT COUNT(1) INTO srccnt
    FROM stg.:sourceobject;

    UPDATE :targetschemaname.:targetobject t
    SET
		entity_id				=	s.entity_id,
		entity_name				=	s.entity_name,
		created_date			=	s.created_date,
		etlactiveind            =	1,
        etljobname              =	p_etljobname,
        envsourcecd             =	p_envsourcecd,
        datasourcecd            =	p_datasourcecd,
        etlupdateddatetime       =	NOW()
    FROM stg.:sourceobject s
    WHERE t.entity_id			=	s.entity_id;

    GET DIAGNOSTICS updcnt = ROW_COUNT;

    INSERT INTO :targetschemaname.:targetobject
    (
    entity_id,
	entity_name,
	created_date,etlactiveind, etljobname, envsourcecd, 
	datasourcecd, etlcreateddatetime
	)

    SELECT
	s.entity_id,
	s.entity_name,
	s.created_date,1, p_etljobname, p_envsourcecd, 
	p_datasourcecd, NOW()
    FROM stg.:sourceobject s
    LEFT JOIN :targetschemaname.:targetobject t
    ON  t.entity_id		=	s.entity_id
    AND	t.entity_name	=	s.entity_name
	WHERE t.entity_id IS NULL;

    GET DIAGNOSTICS inscnt = ROW_COUNT;

--    IF p_rawstorageflag = 1
--    THEN
--
--    INSERT INTO raw.raw_entity
--    (
--    entity_id,
--	entity_name,
--	created_date, etlcreateddatetime
--    )
--    SELECT
--    s.entity_id,
--	s.entity_name,
--	s.created_date, NOW()
--    FROM stg.stg_entity s;
--    
--    END IF;
     EXCEPTION WHEN others THEN
         get stacked diagnostics
             p_errorid   = returned_sqlstate,
             p_errordesc = message_text;
     CALL ods.usp_etlerrorinsert(p_sourceid, p_targetobject, p_dataflowflag, p_batchid,p_taskname, 'sp_ExceptionHandling', p_errorid, p_errordesc, null);
          select 0 into inscnt;
          select 0 into updcnt;
END;
$procedure$
;

"""
