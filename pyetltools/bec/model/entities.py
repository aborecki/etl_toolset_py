from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Boolean,
    ForeignKey,
    DateTime,
    Sequence,
    Float,
    CHAR,
    Date,
    SmallInteger, PrimaryKeyConstraint)
from sqlalchemy.orm import relationship

Base = declarative_base()

class Entity:
    def __repr__(self):
        return "<"+self.__class__.__name__+"({!r})>".format(dict([(i, self.__dict__[i]) for i in self._ordering]))

class ReleaseStatusTableWorkflow(   Base, Entity):
    __tablename__ = 'release_status_table_workflow'
    __table_args__ = (
        PrimaryKeyConstraint('table_id', 'workflow_id'),
        {'schema': 'admin'},
    )
    table_id = Column(BigInteger(), ForeignKey('admin.release_status_table.table_id'))
    workflow_id = Column(BigInteger(), ForeignKey('admin.release_status_workflow.workflow_id'))
    table_type = Column(String(length=128))
    table = relationship("ReleaseStatusTable", back_populates="workflows")
    workflow = relationship("ReleaseStatusWorkflow", back_populates="tables")

    _ordering = ['table_id', 'workflow_id', 'table_type']


class ReleaseStatusTable(Base, Entity):
    __tablename__ = 'release_status_table'
    __table_args__ = {'schema': 'admin'}
    table_id = Column(BigInteger, primary_key=True)
    nz_tablename = Column(String(length=128))
    nz_tableschema = Column(String(length=128))
    ms_tablename = Column(String(length=128))
    ms_dbname = Column(String(length=128))
    rename_status = Column(CHAR(length=1))
    push_to_mssql = Column(CHAR(length=1))
    push_to_mssql_until = Column(Date())
    push_to_nz = Column(CHAR(length=1))
    push_to_nz_until = Column(Date())
    in_datalab = Column(CHAR(length=1))
    in_prod = Column(CHAR(length=1))
    static = Column(CHAR(length=1))
    recon_status = Column(String(length=128))
    last_recon_user = Column(String(length=128))
    comment = Column(String(length=4000))
    is_initable = Column(String(length=30))
    mdw_tables_banknr = Column(String(length=128))
    push_clonevar = Column(String(length=128))
    xyz_kildefact_i = Column(SmallInteger())
    workflows = relationship("ReleaseStatusTableWorkflow",  back_populates="table")

    _ordering = ['table_id', 'nz_tablename', 'nz_tableschema', 'ms_tablename', 'ms_dbname', 'rename_status',
                 'push_to_mssql', 'push_to_mssql_until', 'push_to_nz', 'push_to_nz_until', 'in_datalab', 'in_prod',
                 'static', 'recon_status', 'last_recon_user', 'comment', 'is_initable', 'mdw_tables_banknr',
                 'push_clonevar', 'xyz_kildefact_i']


class ReleaseStatusDetail(Entity, Base):
    __tablename__ = 'release_status_detail'
    __table_args__ = {'schema': 'admin'}
    release= Column( String(length=128), primary_key=True)
    release_date= Column( Date())
    comment= Column( String(length=4000))
    _ordering=['release','release_date','comment']

    workflows = relationship('ReleaseStatusWorkflow', back_populates='release_detail')

class ReleaseStatusWorkflow(Base, Entity):
    __tablename__ = 'release_status_workflow'
    __table_args__ = {'schema': 'admin'}
    workflow_id = Column(BigInteger(), primary_key=True)
    folder_name = Column(String(length=128))
    workflow_name = Column(String(length=128))
    konv_folder_name = Column(String(length=128))
    konv_workflow_name = Column(String(length=128))
    sessions_num = Column(Integer())
    release = Column(String(length=128), ForeignKey('admin.release_status_detail.release'))
    release_date = Column(Date())
    epo_id = Column(String(length=128))
    performance_optimized_by = Column(String(length=128))
    performance_optimized_date = Column(Date())
    test_status = Column(Integer())
    test_responsible = Column(String(length=128))
    tables = relationship('ReleaseStatusTableWorkflow', back_populates='workflow')
    release_detail = relationship('ReleaseStatusDetail', back_populates='workflows')

    _ordering = ['workflow_id', 'folder_name', 'workflow_name', 'konv_folder_name', 'konv_workflow_name',
                  'sessions_num', 'release', 'release_date', 'epo_id', 'performance_optimized_by',
                  'performance_optimized_date', 'test_status', 'test_responsible']




