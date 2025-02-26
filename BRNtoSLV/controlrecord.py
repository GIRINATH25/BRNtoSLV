from sqlalchemy import text
from collections import namedtuple
from db.db_connector import DBConnector

class ControlEntries:
    def __init__(self, dataflowflag=None, sources=None, groups=None, exclude_sources=None, 
                 exclude_groups=None, object_type=None, calling_sequence=None, load_frequency=None, failed_only=False):
        
        self.dataflowflag = dataflowflag
        self.sources = tuple(sources) if sources else None
        self.groups = tuple(groups) if groups else None
        self.exclude_sources = tuple(exclude_sources) if exclude_sources else None
        self.exclude_groups = tuple(exclude_groups) if exclude_groups else None
        self.object_type = tuple(object_type) if object_type else None
        self.calling_sequence = tuple(calling_sequence) if calling_sequence else None
        self.load_frequency = load_frequency
        self.failed_only = failed_only
        engine = DBConnector()
        self.engine = engine.staging()

    def fetch_records(self):
        """
        Executes the query and returns results as named tuples.
        """
        query = self._build_query()
        params = self._build_params()

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            conn.commit()

            Record = namedtuple('Record', result.keys())
            
            return [Record(*row) for row in rows]

    def _build_query(self):
        """
        Builds the dynamic SQL query based on provided filters.
        """
        query = '''
            SELECT
                header.SourceId,
                detail.SourceName,
                detail.SourceType,
                detail.SourceObject,
                header.DepSource,
                detail.LoadType,
                detail.LoadFrequency,
                header.ConnectionStr,
                header.ObjectType,
                header.SourceDelimiter,
                detail.SourceQuery,
                header.APIUrl,
                header.APIMethod,
                header.APIAccessToken,
                header.APIQueryParameters,
                header.APIRequestBody,
                header.ADLSContainerName,
                header.DLDirStructure,
                detail.EtlLastRunDate,
                detail.TargetObject,
                header.SourceCallingSeq,
                detail.DataflowFlag,
                detail.LatestBatchId,
                detail.TargetSchemaName,
                detail.TargetProcedureName,
                detail.IntervalDays
            FROM ods.ControlHeader header
            JOIN ods.ControlDetail detail
                ON header.SourceId = detail.SourceId
            WHERE detail.DataflowFlag = :dataflowflag
            AND detail.IsReadyForExecution = 1
            AND detail.Isapplicable = 1
        '''

        if self.sources:
            query += "\n\tAND detail.SourceId IN :sources"
        elif self.load_frequency:
            query += "\n\tAND detail.LoadFrequency = :load_frequency"

        if self.exclude_sources:
            query += "\n\tAND detail.SourceId NOT IN :exclude_sources"

        if self.groups:
            query += "\n\tAND header.SourceId IN (SELECT DISTINCT SourceId FROM ods.sourcegroupingdtl WHERE schemaname='dwh' AND SourceGroup IN :groups)"

        if self.exclude_groups:
            query += "\n\tAND header.SourceId NOT IN (SELECT DISTINCT SourceId FROM ods.sourcegroupingdtl WHERE schemaname='dwh' AND SourceGroup IN :exclude_groups)"

        if self.object_type:
            query += "\n\tAND header.ObjectType IN :object_type"

        if self.calling_sequence:
            query += "\n\tAND header.SourceCallingSeq IN :calling_sequence"

        if self.failed_only:
            query += "\n\tAND detail.FlowStatus = 'Failed'"

        query += '\n\tORDER BY header.Id'
        return query

    def _build_params(self):
        """
        Builds the query parameters dictionary.
        """
        return {
            'dataflowflag': self.dataflowflag,
            'sources': self.sources,
            'groups': self.groups,
            'exclude_sources': self.exclude_sources,
            'exclude_groups': self.exclude_groups,
            'object_type': self.object_type,
            'calling_sequence': self.calling_sequence,
            'load_frequency': self.load_frequency,
        }
