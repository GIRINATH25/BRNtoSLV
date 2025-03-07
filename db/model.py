from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, CHAR, UniqueConstraint, text, BigInteger, TIMESTAMP
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class ControlHeader(Base):
    __tablename__ = 'controlheader'
    __table_args__ = (
        UniqueConstraint('sourceid', 'sourcename', name='controlheader_ukey'),
        {'schema': 'ods'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    sourcename = Column(String(100), nullable=True)
    sourcetype = Column(String(50), nullable=True)
    sourcedescription = Column(String(100), nullable=True)
    sourceid = Column(String(100), nullable=True)
    connectionstr = Column(String(1000), nullable=True)
    adlscontainername = Column(String(50), nullable=True)
    dwobjectname = Column(String(50), nullable=True)
    objecttype = Column(String(10), nullable=True)
    dldirstructure = Column(String(50), nullable=True)
    dlpurgeflag = Column(CHAR(1), nullable=True)
    dwpurgeflag = Column(CHAR(1), nullable=True)
    ftpcheck = Column(Integer, nullable=False)
    status = Column(String(10), nullable=True)
    createddate = Column(DateTime, nullable=True, default=func.now())
    lastupdateddate = Column(DateTime, nullable=True, onupdate=func.now())
    createduser = Column(String(100), nullable=True)
    isapplicable = Column(Integer, nullable=True)
    profilename = Column(String(100), nullable=True)
    emailto = Column(String(500), nullable=True)
    archcondition = Column(Text, nullable=True)
    depsource = Column(String(100), nullable=True)
    archintvlcond = Column(Text, nullable=True)
    sourcecallingseq = Column(Integer, nullable=True)
    apiurl = Column(String(500), nullable=True)
    apimethod = Column(String(20), nullable=True)
    apiauthorizationtype = Column(String(50), nullable=True)
    apiaccesstoken = Column(String(2000), nullable=True)
    apipymodulename = Column(String(500), nullable=True)
    apiqueryparameters = Column(String(2000), nullable=True)
    apirequestbody = Column(String(2000), nullable=True)
    envsourcecode = Column(String(100), nullable=True)
    datasourcecode = Column(String(100), nullable=True)
    sourcedelimiter = Column(CHAR(10), nullable=True)
    rawstorageflag = Column(Integer, nullable=True, default=1)
    sourcegroup = Column(String(40), nullable=True)
    

class ControlDetail(Base):
    __tablename__ = 'controldetail'
    __table_args__ = (
        UniqueConstraint('sourceid', 'dataflowflag', name='controldetail_ukey'),
        {'schema': 'ods'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    sourcename = Column(String(100), nullable=True)
    sourcetype = Column(String(50), nullable=True)
    sourcedescription = Column(String(100), nullable=False)
    sourceid = Column(String(100), nullable=False)
    sourceschema = Column(String(50), nullable=True)
    sourceobject = Column(String(500), nullable=True)
    dataflowflag = Column(String(50), nullable=True)
    isreadyforexecution = Column(Integer, nullable=True)
    loadtype = Column(String(15), nullable=True)
    loadfrequency = Column(String(10), nullable=True)
    flowstatus = Column(String(10), nullable=True)
    targetname = Column(String(50), nullable=True)
    targetschemaname = Column(String(10), nullable=True)
    targetobject = Column(String(150), nullable=True)
    targetprocedurename = Column(String(150), nullable=True)
    jobname = Column(String(100), nullable=True)
    createddate = Column(DateTime, nullable=True, default=func.now())
    lastupdateddate = Column(DateTime, nullable=True, onupdate=func.now())
    createduser = Column(String(100), nullable=True)
    isapplicable = Column(Integer, nullable=True)
    profilename = Column(String(100), nullable=True)
    emailto = Column(String(500), nullable=True)
    archcondition = Column(Text, nullable=True)
    isdecomreq = Column(Integer, nullable=True)
    archintvlcond = Column(Text, nullable=True)
    sourcequery = Column(String(10000), nullable=True)
    sourcecallingseq = Column(Integer, nullable=True)
    etllastrundate = Column(DateTime, nullable=True)
    latestbatchid = Column(Integer, nullable=True)
    executiontype = Column(String(40), nullable=True)
    intervaldays = Column(Integer, nullable=True)
    

class Audit(Base):
    __tablename__ = 'audit'
    __table_args__ = ({'schema': 'ods'})

    id = Column(Integer, primary_key=True, autoincrement=True)
    sourcename = Column(String(100), nullable=True)
    sourcetype = Column(String(50), nullable=True)
    sourcedescription = Column(String(100), nullable=True)
    sourceobject = Column(String(50), nullable=True)
    sourceid = Column(String(100), nullable=True)
    dataflowflag = Column(String(50), nullable=True)
    srcrwcnt = Column(Integer, nullable=True)
    srcdelcnt = Column(Integer, nullable=True)
    tgtinscnt = Column(Integer, nullable=True)
    tgtdelcnt = Column(Integer, nullable=True)
    tgtupdcnt = Column(Integer, nullable=True)
    flowstatus = Column(String(10), nullable=True)
    targetname = Column(String(50), nullable=True)
    targetschemaname = Column(String(10), nullable=True)
    targetobject = Column(String(150), nullable=True)
    targetprocedurename = Column(String(150), nullable=True)
    createddate = Column(DateTime, nullable=True, default=func.now())
    updateddate = Column(DateTime, nullable=True, onupdate=func.now())
    loadstarttime = Column(DateTime, nullable=True)
    loadendtime = Column(DateTime, nullable=True)
    latestbatchid = Column(Integer, nullable=True)
    loadtype = Column(String(50), nullable=True)
    etlbatchid = Column(Text, nullable=True)
    useragent = Column(Text, nullable=True)
    sourcegroupflag = Column(Integer, nullable=True)





class Error(Base):
    __tablename__ = 'error'
    __table_args__ = ({'schema': 'ods'})

    id = Column(Integer, primary_key=True, autoincrement=True)
    sourcename = Column(String(100), nullable=True)
    sourcetype = Column(String(50), nullable=True)
    sourcedescription = Column(String(100), nullable=True)
    sourceid = Column(String(50), nullable=True)
    sourceobject = Column(String(500), nullable=True)
    dataflowflag = Column(String(50), nullable=True)
    targetname = Column(String(50), nullable=True)
    targetschemaname = Column(String(10), nullable=True)
    targetobject = Column(String(150), nullable=True)
    targetprocedurename = Column(String(150), nullable=True)
    taskname = Column(String(200), nullable=True)
    packagename = Column(String(200), nullable=True)
    jobname = Column(String(100), nullable=True)
    errorid = Column(Integer, nullable=True)
    errordesc = Column(Text, nullable=True)
    errorline = Column(Integer, nullable=True)
    errordate = Column(DateTime, nullable=True, default=func.now())
    latestbatchid = Column(Integer, nullable=True)
    sourcegroupflag = Column(Integer, nullable=True)    



class DwhToClickControlDtl(Base):
    __tablename__ = 'dwhtoclickcontroldtl'
    __table_args__ = {'schema': 'ods'}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    targettablename = Column(String(100), nullable=True)
    dataflowflag = Column(String(20), nullable=True)
    objecttype = Column(String(40), nullable=True)
    objectname = Column(String(100), nullable=True)
    executionflag = Column(Integer, nullable=True)
    seqexecution = Column(Integer, nullable=True)
    loadtype = Column(String(40), nullable=True)
    loadfrequency = Column(String(20), nullable=True)
    status = Column(String(40), nullable=True)
    loadstartdatetime = Column(TIMESTAMP, nullable=True)
    loadenddatetime = Column(TIMESTAMP, nullable=True)
    depsource = Column(String(100), nullable=True)
    createddatetime = Column(TIMESTAMP, server_default=text('now()'), nullable=True)
    sourceschema = Column(String(20), nullable=True)
    targetdatabase = Column(String(1000), nullable=True)
    encrypted_columns = Column(String(1000), nullable=True)

def create_all(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA ods;"))
            conn.commit()
            Base.metadata.create_all(engine)
            print("Tables created successfully")
    except Exception as e:
        print('Already created')